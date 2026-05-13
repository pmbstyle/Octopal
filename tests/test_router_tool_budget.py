from __future__ import annotations

import asyncio

from octopal.infrastructure.providers.base import Message
from octopal.runtime.octo.router import (
    _budget_tool_specs,
    _build_worker_result_payload,
    _decode_and_save_images,
    _expand_active_tool_specs_from_catalog_result,
    _finalize_response,
    _get_heartbeat_tools,
    _get_internal_maintenance_tools,
    _get_octo_tools,
    _get_scheduled_octo_control_tools,
    _get_scheduler_tools,
    _get_worker_followup_tools,
    _normalize_worker_followup_reply,
    _recover_textual_tool_call,
    _sanitize_messages_for_complete,
    _shrink_tool_specs_for_retry,
    route_or_reply,
    route_worker_results_back_to_octo,
)
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.registry import ToolSpec
from octopal.tools.tools import get_tools


def test_budget_keeps_internal_worker_and_scheduler_tools() -> None:
    all_tools = get_tools(mcp_manager=None)
    budgeted = _budget_tool_specs(all_tools, max_count=8)
    names = {spec.name for spec in budgeted}

    must_keep = {
        "check_schedule",
        "start_worker",
        "get_worker_status",
        "get_worker_result",
        "list_workers",
        "list_active_workers",
        "schedule_task",
        "tool_catalog_search",
    }
    assert must_keep.issubset(names)


def test_shrink_retry_keeps_start_worker() -> None:
    all_tools = get_tools(mcp_manager=None)
    shrunk = _shrink_tool_specs_for_retry(all_tools)
    names = {spec.name for spec in shrunk}
    assert "start_worker" in names


def test_get_octo_tools_uses_small_core_and_defers_mcp_tools(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    class DummyOcto:
        mcp_manager = None

    def fake_get_tools(mcp_manager=None):
        base = get_tools(mcp_manager=None)
        mcp_tools = [
            ToolSpec(
                name=f"mcp_demo_tool_{index}",
                description="demo mcp tool",
                parameters={"type": "object", "properties": {}, "additionalProperties": False},
                permission="mcp_exec",
                handler=lambda _args, _ctx: {"ok": True},
                is_async=True,
            )
            for index in range(12)
        ]
        return base + mcp_tools

    monkeypatch.setattr(router, "get_tools", fake_get_tools)
    monkeypatch.delenv("OCTOPAL_OCTO_DEFER_TOOL_LOADING", raising=False)
    monkeypatch.delenv("OCTOPAL_OCTO_MAX_INITIAL_TOOL_COUNT", raising=False)

    tool_specs, ctx = _get_octo_tools(DummyOcto(), 0)
    names = {spec.name for spec in tool_specs}
    all_names = {spec.name for spec in ctx["all_tool_specs"]}

    assert "tool_catalog_search" in names
    assert "start_worker" in names
    assert "fs_read" in names
    assert "octo_opportunity_scan" in names
    assert "octo_self_queue_add" in names
    assert "execute_self_queue_item" in names
    assert "octo_self_queue_list" in names
    assert "octo_self_queue_take" in names
    assert "octo_self_queue_update" in names
    assert "repair_scheduled_tasks" in names
    assert "octo_restart_self" in names
    assert "octo_check_update" in names
    assert "octo_update_self" in names
    assert "mcp_demo_tool_0" not in names
    assert "mcp_demo_tool_0" in all_names
    assert len(tool_specs) < len(all_names)


def test_get_octo_tools_keeps_self_lifecycle_tools_with_profile(monkeypatch) -> None:
    class DummyOcto:
        mcp_manager = None

    monkeypatch.setenv("OCTOPAL_OCTO_TOOL_PROFILE", "research")
    monkeypatch.delenv("OCTOPAL_OCTO_DEFER_TOOL_LOADING", raising=False)

    tool_specs, _ctx = _get_octo_tools(DummyOcto(), 0)
    names = {spec.name for spec in tool_specs}

    assert "octo_restart_self" in names
    assert "octo_check_update" in names
    assert "octo_update_self" in names


def test_worker_followup_tools_are_narrow(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    def fake_get_tools(mcp_manager=None):
        return [
            ToolSpec(
                name="start_worker",
                description="start",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="manage_canon",
                description="canon",
                parameters={"type": "object", "properties": {}},
                permission="canon_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="get_worker_output_path",
                description="worker output",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="fs_read",
                description="fs read",
                parameters={"type": "object", "properties": {}},
                permission="filesystem_read",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="fs_write",
                description="fs write",
                parameters={"type": "object", "properties": {}},
                permission="filesystem_write",
                handler=lambda args, ctx: {"ok": True},
            ),
        ]

    class DummyOcto:
        mcp_manager = None

    monkeypatch.setattr(router, "get_tools", fake_get_tools)

    tools, _ctx = _get_worker_followup_tools(DummyOcto(), 123)

    assert {tool.name for tool in tools} == {"manage_canon", "get_worker_output_path", "fs_write"}


def test_control_plane_tools_do_not_hydrate_dynamic_mcp_catalog(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    calls: list[object] = []

    def fake_get_tools(mcp_manager=None):
        calls.append(mcp_manager)
        return [
            ToolSpec(
                name="octo_context_health",
                description="health",
                parameters={"type": "object", "properties": {}},
                permission="service_read",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="scheduler_status",
                description="scheduler",
                parameters={"type": "object", "properties": {}},
                permission="service_read",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="check_schedule",
                description="schedule",
                parameters={"type": "object", "properties": {}},
                permission="service_read",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="gateway_status",
                description="gateway",
                parameters={"type": "object", "properties": {}},
                permission="service_read",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="list_workers",
                description="list workers",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="list_active_workers",
                description="list active workers",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="manage_canon",
                description="manage canon",
                parameters={"type": "object", "properties": {}},
                permission="canon_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="search_canon",
                description="search canon",
                parameters={"type": "object", "properties": {}},
                permission="canon_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="mcp_agentmail_list_inboxes",
                description="dynamic mcp",
                parameters={"type": "object", "properties": {}},
                permission="mcp_exec",
                handler=lambda args, ctx: {"ok": True},
            ),
        ]

    class NoisyMCPManager:
        def get_all_tools(self):
            raise AssertionError("control-plane route should not inspect dynamic MCP tools")

    class DummyOcto:
        mcp_manager = NoisyMCPManager()

    monkeypatch.setattr(router, "get_tools", fake_get_tools)

    heartbeat_tools, heartbeat_ctx = _get_heartbeat_tools(DummyOcto(), 123)
    scheduler_tools, scheduler_ctx = _get_scheduler_tools(DummyOcto(), 123)
    scheduled_octo_control_tools, scheduled_octo_control_ctx = _get_scheduled_octo_control_tools(
        DummyOcto(), 123
    )
    internal_maintenance_tools, internal_maintenance_ctx = _get_internal_maintenance_tools(
        DummyOcto(), 123
    )

    assert calls == [None, None, None, None]
    assert heartbeat_ctx["mcp_refresh_attempted"] is False
    assert scheduler_ctx["mcp_refresh_attempted"] is False
    assert scheduled_octo_control_ctx["mcp_refresh_attempted"] is False
    assert internal_maintenance_ctx["mcp_refresh_attempted"] is False
    assert "mcp_agentmail_list_inboxes" not in {tool.name for tool in heartbeat_tools}
    assert "mcp_agentmail_list_inboxes" not in {tool.name for tool in scheduler_tools}
    assert "mcp_agentmail_list_inboxes" not in {
        tool.name for tool in scheduled_octo_control_tools
    }
    assert "mcp_agentmail_list_inboxes" not in {
        tool.name for tool in internal_maintenance_tools
    }
    assert {"list_workers", "list_active_workers"}.issubset(
        {tool.name for tool in scheduler_tools}
    )
    assert {
        "list_workers",
        "list_active_workers",
        "manage_canon",
        "search_canon",
    }.issubset({tool.name for tool in scheduled_octo_control_tools})
    assert {"list_workers", "list_active_workers"}.issubset(
        {tool.name for tool in internal_maintenance_tools}
    )


def test_bounded_route_allowlists_resolve_without_permission_blocks() -> None:
    import octopal.runtime.octo.router as router

    class DummyOcto:
        mcp_manager = None

    routes = [
        (
            "worker_followup",
            router._get_worker_followup_tools,
            router._WORKER_FOLLOWUP_ALLOWED_TOOL_NAMES,
        ),
        ("heartbeat", router._get_heartbeat_tools, router._HEARTBEAT_ALLOWED_TOOL_NAMES),
        ("scheduler", router._get_scheduler_tools, router._SCHEDULER_ALLOWED_TOOL_NAMES),
        ("proactive", router._get_proactive_tools, router._PROACTIVE_ALLOWED_TOOL_NAMES),
        (
            "scheduled_octo_control",
            router._get_scheduled_octo_control_tools,
            router._SCHEDULED_OCTO_CONTROL_ALLOWED_TOOL_NAMES,
        ),
        (
            "internal_maintenance",
            router._get_internal_maintenance_tools,
            router._INTERNAL_MAINTENANCE_ALLOWED_TOOL_NAMES,
        ),
    ]

    for route_name, tool_factory, allowed_names in routes:
        tools, ctx = tool_factory(DummyOcto(), 0)
        available_names = {tool.name for tool in tools}
        assert set(allowed_names) <= available_names, route_name

        blocked_allowed = {
            tool.name: tool.reasons
            for tool in ctx["tool_resolution_report"].blocked_tools
            if tool.name in allowed_names
        }
        assert blocked_allowed == {}, route_name


def test_worker_followup_tools_do_not_hydrate_dynamic_mcp_catalog(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    calls: list[object] = []

    def fake_get_tools(mcp_manager=None):
        calls.append(mcp_manager)
        return [
            ToolSpec(
                name="manage_canon",
                description="canon",
                parameters={"type": "object", "properties": {}},
                permission="canon_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="get_worker_output_path",
                description="worker output",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="fs_write",
                description="fs write",
                parameters={"type": "object", "properties": {}},
                permission="filesystem_write",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="mcp_agentmail_list_inboxes",
                description="dynamic mcp",
                parameters={"type": "object", "properties": {}},
                permission="mcp_exec",
                handler=lambda args, ctx: {"ok": True},
            ),
        ]

    class NoisyMCPManager:
        def get_all_tools(self):
            raise AssertionError("worker follow-up should not inspect dynamic MCP tools")

    class DummyOcto:
        mcp_manager = NoisyMCPManager()

    monkeypatch.setattr(router, "get_tools", fake_get_tools)

    tools, ctx = _get_worker_followup_tools(DummyOcto(), 123)

    assert calls == [None]
    assert ctx["mcp_refresh_attempted"] is False
    assert {tool.name for tool in tools} == {"manage_canon", "get_worker_output_path", "fs_write"}


def test_worker_followup_route_skips_planner_and_uses_narrow_tools(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    class DummyProvider:
        def __init__(self) -> None:
            self.tool_snapshots: list[list[str]] = []

        async def complete(self, messages, **kwargs):
            raise AssertionError("follow-up route should use tool-capable path first in this test")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_snapshots.append([tool["function"]["name"] for tool in tools])
            return {"content": "NO_USER_RESPONSE", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        facts = None
        reflection = None
        is_ws_active = False
        mcp_manager = None

        def __init__(self) -> None:
            self.provider = DummyProvider()
            self.memory = DummyMemory()
            self.thinking_states: list[bool] = []

        async def set_thinking(self, active: bool) -> None:
            self.thinking_states.append(active)

    def fake_get_tools(mcp_manager=None):
        return [
            ToolSpec(
                name="start_worker",
                description="start",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="manage_canon",
                description="canon",
                parameters={"type": "object", "properties": {}},
                permission="canon_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="get_worker_output_path",
                description="worker output",
                parameters={"type": "object", "properties": {}},
                permission="worker_manage",
                handler=lambda args, ctx: {"ok": True},
            ),
            ToolSpec(
                name="fs_write",
                description="fs write",
                parameters={"type": "object", "properties": {}},
                permission="filesystem_write",
                handler=lambda args, ctx: {"ok": True},
            ),
        ]

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_bootstrap_context_prompt(store, chat_id):
        return Message(role="system", content="bootstrap")

    async def fake_build_plan(provider, messages, has_tools):
        raise AssertionError("planner should not run for worker follow-up route")

    monkeypatch.setattr(router, "get_tools", fake_get_tools)
    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(
        router, "build_bootstrap_context_prompt", fake_build_bootstrap_context_prompt
    )
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        octo = DummyOcto()
        response = await route_worker_results_back_to_octo(
            octo,
            123,
            [
                (
                    "worker-1",
                    "summarize findings",
                    WorkerResult(
                        summary="done",
                        output={
                            "report_path": "reports/out.md",
                            "durable_paths": ["reports/out.md"],
                        },
                    ),
                )
            ],
        )
        assert response == "NO_USER_RESPONSE"
        assert len(octo.provider.tool_snapshots) == 1
        assert set(octo.provider.tool_snapshots[0]) == {
            "get_worker_output_path",
            "manage_canon",
            "fs_write",
        }
        assert octo.thinking_states == [True, False]

    asyncio.run(scenario())


def test_normalize_worker_followup_reply_uses_structured_user_response() -> None:
    raw = """
    {
      "user_response": "Брифинг готов.",
      "no_user_response": false,
      "actions_taken": [{"type": "get_worker_output_path", "summary": "checked output"}],
      "reason": "worker completed"
    }
    """
    assert _normalize_worker_followup_reply(raw) == "Брифинг готов."


def test_normalize_worker_followup_reply_strips_noisy_user_visible_wrapper() -> None:
    raw = (
        "I checked internal worker state and should only show the marked part.\n\n"
        "<user_visible>Брифинг готов.</user_visible>"
    )
    assert _normalize_worker_followup_reply(raw) == "Брифинг готов."


def test_normalize_worker_followup_reply_suppresses_structured_no_response() -> None:
    raw = '{"user_response": null, "no_user_response": true, "actions_taken": [], "reason": "saved memory"}'
    assert _normalize_worker_followup_reply(raw) == "NO_USER_RESPONSE"


def test_normalize_worker_followup_reply_suppresses_embedded_no_response_json() -> None:
    raw = """
    I should not expose this internal reasoning.

    ```json
    {
      "user_response": null,
      "no_user_response": true,
      "actions_taken": [{"type": "canon_verify", "summary": "checked facts.md"}],
      "reason": "internal bookkeeping only"
    }
    ```

    More internal reasoning that should not reach the user.
    """
    assert _normalize_worker_followup_reply(raw) == "NO_USER_RESPONSE"


def test_build_worker_result_payload_keeps_preview_text_for_large_output() -> None:
    payload = _build_worker_result_payload(
        "worker-1",
        "collect data",
        WorkerResult(
            summary="done",
            output={
                "report_path": "reports/out.md",
                "durable_paths": ["reports/out.md"],
                "results": [{"body": "x" * 2000, "idx": idx} for idx in range(80)],
            },
        ),
    )

    assert payload["worker_id"] == "worker-1"
    assert payload["output_truncated"] is True
    assert payload["output"] == {"available_keys": ["report_path", "durable_paths", "results"]}
    assert payload["output_preview_text"]
    assert payload["output_chars"] > 64000
    assert "report_path" in payload["output_preview_text"]
    assert "results" in payload["output_preview_text"]
    assert payload["artifact_summary"]["primary_report_path"] == "reports/out.md"
    assert payload["artifact_summary"]["durable_paths"] == ["reports/out.md"]


def test_catalog_result_expands_active_tool_specs() -> None:
    active = [
        ToolSpec(
            name="tool_catalog_search",
            description="catalog",
            parameters={"type": "object", "properties": {}},
            permission="self_control",
            handler=lambda args, ctx: "{}",
        )
    ]
    hidden = ToolSpec(
        name="hidden_tool",
        description="hidden",
        parameters={"type": "object", "properties": {}},
        permission="self_control",
        handler=lambda args, ctx: {"ok": True},
    )

    updated, expanded = _expand_active_tool_specs_from_catalog_result(
        {
            "results": [
                {"name": "hidden_tool", "active_now": False},
                {"name": "tool_catalog_search", "active_now": True},
            ]
        },
        active_tool_specs=active,
        ctx={"all_tool_specs": active + [hidden]},
    )

    assert expanded == ["hidden_tool"]
    assert {spec.name for spec in updated} == {"tool_catalog_search", "hidden_tool"}


def test_catalog_result_expands_only_exact_mcp_match() -> None:
    active = [
        ToolSpec(
            name="tool_catalog_search",
            description="catalog",
            parameters={"type": "object", "properties": {}},
            permission="self_control",
            handler=lambda args, ctx: "{}",
        )
    ]
    exact = ToolSpec(
        name="mcp_drive_search_files",
        description="Search Drive files",
        parameters={"type": "object", "properties": {}},
        permission="mcp_exec",
        handler=lambda args, ctx: {"ok": True},
        is_async=True,
        server_id="drive",
        remote_tool_name="search_files",
    )
    sibling = ToolSpec(
        name="mcp_drive_list_files",
        description="List Drive files",
        parameters={"type": "object", "properties": {}},
        permission="mcp_exec",
        handler=lambda args, ctx: {"ok": True},
        is_async=True,
        server_id="drive",
        remote_tool_name="list_files",
    )

    updated, expanded = _expand_active_tool_specs_from_catalog_result(
        {
            "query": "search_files",
            "results": [
                {
                    "name": "mcp_drive_search_files",
                    "active_now": False,
                    "is_mcp": True,
                    "server_id": "drive",
                    "remote_name": "search_files",
                    "owner": "mcp",
                },
                {
                    "name": "mcp_drive_list_files",
                    "active_now": False,
                    "is_mcp": True,
                    "server_id": "drive",
                    "remote_name": "list_files",
                    "owner": "mcp",
                },
            ],
        },
        active_tool_specs=active,
        ctx={"all_tool_specs": active + [exact, sibling]},
    )

    assert expanded == ["mcp_drive_search_files"]
    assert {spec.name for spec in updated} == {"tool_catalog_search", "mcp_drive_search_files"}


def test_catalog_result_does_not_auto_expand_broad_mcp_search() -> None:
    active = [
        ToolSpec(
            name="tool_catalog_search",
            description="catalog",
            parameters={"type": "object", "properties": {}},
            permission="self_control",
            handler=lambda args, ctx: "{}",
        )
    ]
    broad_matches = [
        ToolSpec(
            name=f"mcp_gmail_tool_{index}",
            description="gmail mcp tool",
            parameters={"type": "object", "properties": {}},
            permission="mcp_exec",
            handler=lambda args, ctx: {"ok": True},
            is_async=True,
            server_id="gmail",
            remote_tool_name=f"tool_{index}",
        )
        for index in range(3)
    ]

    updated, expanded = _expand_active_tool_specs_from_catalog_result(
        {
            "query": "gmail",
            "results": [
                {
                    "name": spec.name,
                    "active_now": False,
                    "is_mcp": True,
                    "server_id": "gmail",
                    "remote_name": spec.remote_tool_name,
                    "owner": "mcp",
                    "description": spec.description,
                }
                for spec in broad_matches
            ],
        },
        active_tool_specs=active,
        ctx={"all_tool_specs": active + broad_matches},
    )

    assert expanded == []
    assert {spec.name for spec in updated} == {"tool_catalog_search"}


def test_catalog_result_hydrates_selected_mcp_tool_from_manager() -> None:
    active = [
        ToolSpec(
            name="tool_catalog_search",
            description="catalog",
            parameters={"type": "object", "properties": {}},
            permission="self_control",
            handler=lambda args, ctx: "{}",
        )
    ]
    compact_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    full_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 3},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50},
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    mcp_tool = ToolSpec(
        name="mcp_drive_search_files",
        description="Search Drive files",
        parameters=compact_schema,
        permission="mcp_exec",
        handler=lambda args, ctx: {"ok": True},
        is_async=True,
        server_id="drive",
        remote_tool_name="search_files",
    )

    class _MCPManager:
        def hydrate_tool_spec(self, spec):
            return ToolSpec(
                name=spec.name,
                description=spec.description,
                parameters=full_schema,
                permission=spec.permission,
                handler=spec.handler,
                is_async=spec.is_async,
                server_id=spec.server_id,
                remote_tool_name=spec.remote_tool_name,
                metadata=spec.metadata,
            )

    updated, expanded = _expand_active_tool_specs_from_catalog_result(
        {
            "query": "search_files",
            "results": [
                {
                    "name": "mcp_drive_search_files",
                    "active_now": False,
                    "is_mcp": True,
                    "server_id": "drive",
                    "remote_name": "search_files",
                    "owner": "mcp",
                }
            ],
        },
        active_tool_specs=active,
        ctx={"all_tool_specs": active + [mcp_tool], "mcp_manager": _MCPManager()},
    )

    assert expanded == ["mcp_drive_search_files"]
    selected = next(spec for spec in updated if spec.name == "mcp_drive_search_files")
    assert selected.parameters == full_schema


def test_route_falls_back_when_tool_run_ends_with_empty_response(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0

        async def complete(self, messages, **kwargs):
            return "I checked it and I'm still working through the result."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
            if self.tool_calls == 1:
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "dummy_tool", "arguments": "{}"},
                        }
                    ],
                }
            return {"content": "", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        def __init__(self) -> None:
            self.thinking_states: list[bool] = []
            self.typing_states: list[tuple[int, bool]] = []

        async def set_typing(self, chat_id: int, active: bool) -> None:
            self.typing_states.append((chat_id, active))

        async def set_thinking(self, active: bool) -> None:
            self.thinking_states.append(active)

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return {"mode": "execute", "steps": ["run dummy tool"], "response": ""}

    def dummy_tool(args, ctx):
        return {"ok": True}

    def fake_get_octo_tools(octo, chat_id):
        return (
            [
                ToolSpec(
                    name="dummy_tool",
                    description="dummy",
                    parameters={"type": "object", "properties": {}},
                    permission="exec",
                    handler=dummy_tool,
                )
            ],
            {"octo": octo, "chat_id": chat_id},
        )

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_octo_tools", fake_get_octo_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        octo = DummyOcto()
        response = await router.route_or_reply(
            octo,
            provider,
            DummyMemory(),
            "check this",
            123,
            "",
        )
        assert response == "I checked it and I'm still working through the result."
        assert provider.tool_calls == 2

    asyncio.run(scenario())


def test_route_retries_image_message_with_saved_file_paths(monkeypatch, tmp_path) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0
            self.last_retry_messages = None

        async def complete(self, messages, **kwargs):
            return "I could not use tools, but I preserved the image locally and explained the limitation."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
            if self.tool_calls == 1:
                raise RuntimeError(
                    "OpenAIException - Invalid API parameter. {'error': {'code': '1210'}}"
                )
            self.last_retry_messages = messages
            return {"content": "I inspected the saved image path via tools.", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [
            Message(
                role="user",
                content=[
                    {"type": "text", "text": str(kwargs["user_text"])},
                    {"type": "image_url", "image_url": {"url": kwargs["images"][0]}},
                ],
            )
        ]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    async def scenario() -> None:
        provider = DummyProvider()
        response = await router.route_or_reply(
            DummyOcto(),
            provider,
            DummyMemory(),
            "what is in this image?",
            123,
            "",
            images=["data:image/jpeg;base64,SGVsbG8="],
            saved_file_paths=[str(tmp_path / "existing.jpg")],
        )
        assert response == "I inspected the saved image path via tools."
        assert provider.tool_calls == 2
        assert provider.last_retry_messages is not None
        last_message = provider.last_retry_messages[-1]
        assert last_message["role"] == "user"
        assert "saved locally for tool-based inspection" in last_message["content"]
        assert str(tmp_path / "existing.jpg") in last_message["content"]

    asyncio.run(scenario())


def test_image_fallback_without_saved_paths_uses_channel_neutral_directory(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    saved_paths = _decode_and_save_images(["data:image/jpeg;base64,SGVsbG8="])

    assert len(saved_paths) == 1
    saved_path = saved_paths[0]
    assert "tmp/incoming_images" in saved_path.replace("\\", "/")
    assert "telegram_images" not in saved_path.replace("\\", "/")


def test_route_retries_unbacked_action_commitment_with_tools(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.retry_prompt_seen = False
            self.verifier_seen = False

        async def complete(self, messages, **kwargs):
            self.verifier_seen = any(
                "Classify whether the draft assistant response is safe to deliver"
                in str(message.get("content", ""))
                for message in messages
            )
            assert self.verifier_seen
            return '{"verdict":"requires_runtime_action_state","confidence":0.91,"reason":"draft needs action state"}'

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {"content": "Проверю это сейчас.", "tool_calls": []}
            if self.calls == 2:
                self.retry_prompt_seen = any(
                    "previous answer was classified as requiring concrete runtime action state"
                    in str(getattr(message, "content", ""))
                    for message in messages
                )
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "dummy_tool", "arguments": "{}"},
                        }
                    ],
                }
            return {"content": "Проверила: ok.", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    def fake_get_octo_tools(octo, chat_id):
        def dummy_tool(args, ctx):
            return {"status": "ok"}

        return [
            ToolSpec(
                name="dummy_tool",
                description="dummy",
                parameters={"type": "object", "properties": {}, "additionalProperties": False},
                permission="exec",
                handler=dummy_tool,
            )
        ], {"octo": octo, "chat_id": chat_id}

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_octo_tools", fake_get_octo_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        response = await router.route_or_reply(
            DummyOcto(),
            provider,
            DummyMemory(),
            "проверь статус",
            123,
            "",
        )
        assert response == "Проверила: ok."
        assert provider.calls == 3
        assert provider.verifier_seen is True
        assert provider.retry_prompt_seen is True

    asyncio.run(scenario())


def test_route_retries_with_fewer_tools_after_invalid_tool_payload(monkeypatch) -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.tool_counts: list[int] = []

        async def complete(self, messages, **kwargs):
            return "Fallback text should not be used."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_counts.append(len(tools))
            if len(tools) > 12:
                raise RuntimeError(
                    "OpenAIException - Invalid API parameter. {'error': {'code': '1210'}}"
                )
            return {"content": "Recovered after shrinking tool set.", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    def fake_get_octo_tools(octo, chat_id):
        tools = [
            ToolSpec(
                name=f"dummy_tool_{idx}",
                description="dummy",
                parameters={"type": "object", "properties": {}},
                permission="exec",
                handler=lambda args, ctx: {"ok": True},
            )
            for idx in range(20)
        ]
        return tools, {"octo": octo, "chat_id": chat_id}

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_octo_tools", fake_get_octo_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        response = await router.route_or_reply(
            DummyOcto(),
            provider,
            DummyMemory(),
            "check this",
            123,
            "",
        )
        assert response == "Recovered after shrinking tool set."
        assert provider.tool_counts[:2] == [20, 14]
        assert provider.tool_counts[-1] == 12

    asyncio.run(scenario())


def test_route_passes_saved_file_paths_into_prompt(monkeypatch) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "Looks good."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    captured_kwargs = {}

    async def fake_build_octo_prompt(**kwargs):
        captured_kwargs.update(kwargs)
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        response = await router.route_or_reply(
            DummyOcto(),
            DummyProvider(),
            DummyMemory(),
            "what is in this image?",
            123,
            "",
            images=["data:image/jpeg;base64,SGVsbG8="],
            saved_file_paths=["/tmp/telegram_images/img_test.jpg"],
        )
        assert response == "Looks good."
        assert captured_kwargs["saved_file_paths"] == ["/tmp/telegram_images/img_test.jpg"]

    asyncio.run(scenario())


def test_plain_completion_does_not_stream_for_telegram(monkeypatch) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "Final reply"

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("telegram path should not use streaming partials")

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        is_ws_active = False
        internal_progress_send = object()

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        response = await router.route_or_reply(
            DummyOcto(),
            DummyProvider(),
            DummyMemory(),
            "hello",
            123,
            "",
        )
        assert response == "Final reply"

    asyncio.run(scenario())


def test_plain_completion_can_stream_for_websocket(monkeypatch) -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("websocket path should prefer streaming partials")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            await on_partial("partial text")
            return "Final reply"

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        is_ws_active = True

        def __init__(self) -> None:
            self.progress: list[tuple[str, str]] = []

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

        async def internal_progress_send(
            self, chat_id: int, state: str, text: str, meta: dict
        ) -> None:
            self.progress.append((state, text))

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)

    async def scenario() -> None:
        octo = DummyOcto()
        response = await router.route_or_reply(
            octo,
            DummyProvider(),
            DummyMemory(),
            "hello",
            123,
            "",
        )
        assert response == "Final reply"
        assert octo.progress == [("partial", "partial text")]

    asyncio.run(scenario())


def test_recover_textual_tool_name_without_args() -> None:
    spec = ToolSpec(
        name="check_schedule",
        description="check schedule",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="self_control",
        handler=lambda args, ctx: {"ok": True},
    )

    recovered = _recover_textual_tool_call("check_schedule", [spec])
    assert recovered is not None
    assert recovered["function"]["name"] == "check_schedule"
    assert recovered["function"]["arguments"] == "{}"


def test_recover_textual_tool_preview_with_file_alias() -> None:
    spec = ToolSpec(
        name="fs_read",
        description="read file",
        parameters={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
        permission="filesystem_read",
        handler=lambda args, ctx: {"ok": True},
    )

    recovered = _recover_textual_tool_call("fs_read, file: memory/2026-03-11.md", [spec])
    assert recovered is not None
    assert recovered["function"]["name"] == "fs_read"
    assert recovered["function"]["arguments"] == '{"path": "memory/2026-03-11.md"}'


def test_do_not_recover_human_text_wrapped_around_tool_name() -> None:
    spec = ToolSpec(
        name="check_schedule",
        description="check schedule",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="self_control",
        handler=lambda args, ctx: {"ok": True},
    )

    assert _recover_textual_tool_call("Checking schedule... check_schedule", [spec]) is None


def test_sanitize_messages_keeps_tool_results_for_plain_fallback() -> None:
    sanitized = _sanitize_messages_for_complete(
        [
            {"role": "system", "content": "Use tool results."},
            {"role": "assistant", "content": "", "tool_calls": [{"id": "call-1"}]},
            {"role": "tool", "name": "check_schedule", "content": '{"status":"ok","tasks":[]}'},
        ]
    )

    tool_summary = next(
        msg for msg in sanitized if msg["role"] == "assistant" and "Tool result" in msg["content"]
    )
    assert "Tool result (check_schedule)" in tool_summary["content"]
    assert '"status": "ok"' in tool_summary["content"]


def test_finalize_response_rewrites_bare_tool_name() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            assert any(
                "collapsed into a tool invocation" in str(m.get("content", "")) for m in messages
            )
            return "I checked the worker list and the system is ready."

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "list_workers",
            internal_followup=False,
        )
        assert result == "I checked the worker list and the system is ready."

    asyncio.run(scenario())


def test_finalize_response_returns_no_user_response_when_rewrite_still_bad() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "check_schedule"

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "list_workers",
            internal_followup=True,
        )
        assert result == "NO_USER_RESPONSE"

    asyncio.run(scenario())


def test_finalize_response_preserves_control_token_without_rewrite() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("control token should not trigger rewrite")

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "NO_USER_RESPONSE",
            internal_followup=True,
        )
        assert result == "NO_USER_RESPONSE"

    asyncio.run(scenario())


def test_finalize_response_preserves_scheduled_completion_token_without_rewrite() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("scheduled completion token should not trigger rewrite")

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "SCHEDULED_TASK_DONE",
            internal_followup=False,
        )
        assert result == "SCHEDULED_TASK_DONE"

    asyncio.run(scenario())


def test_finalize_response_preserves_scheduler_idle_token_without_rewrite() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("scheduler idle token should not trigger rewrite")

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "SCHEDULER_IDLE",
            internal_followup=False,
        )
        assert result == "SCHEDULER_IDLE"

    asyncio.run(scenario())


def test_finalize_response_preserves_scheduled_blocked_token_without_rewrite() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("scheduled blocked token should not trigger rewrite")

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "SCHEDULED_TASK_BLOCKED",
            internal_followup=False,
        )
        assert result == "SCHEDULED_TASK_BLOCKED"

    asyncio.run(scenario())


def test_route_can_expand_toolset_after_catalog_search(monkeypatch) -> None:
    hidden_tool = ToolSpec(
        name="hidden_tool",
        description="A hidden tool revealed by catalog search.",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="self_control",
        handler=lambda args, ctx: {"ok": True, "used": "hidden_tool"},
    )

    catalog_tool = ToolSpec(
        name="tool_catalog_search",
        description="catalog",
        parameters={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "additionalProperties": False,
        },
        permission="self_control",
        handler=lambda args, ctx: '{"status":"ok","results":[{"name":"hidden_tool","active_now":false}]}',
    )

    class DummyProvider:
        def __init__(self) -> None:
            self.tool_snapshots: list[list[str]] = []
            self.calls = 0

        async def complete(self, messages, **kwargs):
            raise AssertionError("plain completion should not be used in this scenario")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this scenario")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            names = [tool["function"]["name"] for tool in tools]
            self.tool_snapshots.append(names)
            self.calls += 1
            if self.calls == 1:
                assert "tool_catalog_search" in names
                assert "hidden_tool" not in names
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {
                                "name": "tool_catalog_search",
                                "arguments": '{"query":"hidden tool"}',
                            },
                        }
                    ],
                }
            if self.calls == 2:
                assert "hidden_tool" in names
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-2",
                            "type": "function",
                            "function": {"name": "hidden_tool", "arguments": "{}"},
                        }
                    ],
                }
            return {"content": "Expanded tool worked.", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    def fake_get_octo_tools(octo, chat_id):
        active_tools = [catalog_tool]
        all_tools = [catalog_tool, hidden_tool]
        return active_tools, {
            "octo": octo,
            "chat_id": chat_id,
            "active_tool_specs": active_tools,
            "all_tool_specs": all_tools,
        }

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_octo_tools", fake_get_octo_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        response = await route_or_reply(
            DummyOcto(),
            provider,
            DummyMemory(),
            "use the hidden tool",
            123,
            "",
        )
        assert response == "Expanded tool worked."
        assert len(provider.tool_snapshots) == 3
        assert "hidden_tool" in provider.tool_snapshots[1]

    asyncio.run(scenario())


def test_route_marks_structured_followup_requirement_from_tool_payload(monkeypatch) -> None:
    worker_tool = ToolSpec(
        name="start_worker",
        description="Start a worker.",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
        permission="worker_manage",
        handler=lambda args, ctx: (
            '{"status":"started","worker_id":"run-1","run_id":"run-1",'
            '"followup_required":true,"next_best_action":"wait_for_worker_progress"}'
        ),
    )

    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def complete(self, messages, **kwargs):
            raise AssertionError("plain completion should not be used in this scenario")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this scenario")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "start_worker", "arguments": "{}"},
                        }
                    ],
                }
            return {"content": "Проверяю и вернусь с итогом.", "tool_calls": []}

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False

        def __init__(self) -> None:
            self.followup_marked = 0

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

        def mark_structured_followup_required(self, correlation_id=None) -> None:
            del correlation_id
            self.followup_marked += 1

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    def fake_get_octo_tools(octo, chat_id):
        return [worker_tool], {
            "octo": octo,
            "chat_id": chat_id,
            "active_tool_specs": [worker_tool],
            "all_tool_specs": [worker_tool],
        }

    import octopal.runtime.octo.router as router

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_get_octo_tools", fake_get_octo_tools)

    async def scenario() -> None:
        provider = DummyProvider()
        octo = DummyOcto()
        response = await route_or_reply(
            octo,
            provider,
            DummyMemory(),
            "check it",
            123,
            "",
        )
        assert response == "Проверяю и вернусь с итогом."
        assert octo.followup_marked == 1

    asyncio.run(scenario())


def test_finalize_response_returns_no_user_response_when_non_followup_rewrite_still_bad() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            return "check_schedule"

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "list_workers",
            internal_followup=False,
        )
        assert result == "NO_USER_RESPONSE"

    asyncio.run(scenario())


def test_finalize_response_preserves_reaction_only_reply() -> None:
    class DummyProvider:
        async def complete(self, messages, **kwargs):
            raise AssertionError("reaction-only reply should not trigger rewrite")

    async def scenario() -> None:
        result = await _finalize_response(
            DummyProvider(),
            [Message(role="system", content="Rewrite if needed.")],
            "<react>👍</react>",
            internal_followup=False,
        )
        assert result == "<react>👍</react>"

    asyncio.run(scenario())
