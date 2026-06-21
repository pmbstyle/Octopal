from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from octopal.infrastructure.config.models import A2AConfig, A2APeerConfig
from octopal.infrastructure.providers.base import Message
from octopal.runtime.octo.delivery import (
    resolve_user_delivery,
    restore_user_delivery,
    suppress_user_delivery,
)
from octopal.runtime.octo.router import (
    RuntimeActionContract,
    _budget_tool_specs,
    _build_worker_result_payload,
    _complete_route_with_tools,
    _decode_and_save_images,
    _expand_active_tool_specs_from_catalog_result,
    _finalize_response,
    _get_heartbeat_tools,
    _get_internal_maintenance_tools,
    _get_octo_tools,
    _get_scheduled_octo_control_tools,
    _get_scheduler_tools,
    _get_worker_followup_tools,
    _needs_action_or_blocked_retry,
    _normalize_worker_followup_reply,
    _recover_textual_tool_call,
    _sanitize_messages_for_complete,
    _shrink_tool_specs_for_retry,
    _update_runtime_action_contracts,
    route_or_reply,
    route_worker_results_back_to_octo,
)
from octopal.runtime.workers.contracts import WorkerResult
from octopal.tools.communication.send_file import send_file_to_user
from octopal.tools.registry import ToolSpec
from octopal.tools.tools import get_tools

_RUNTIME_PLAN_TOOL_NAMES = {"plan_create", "plan_status", "plan_update_step"}


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
        *_RUNTIME_PLAN_TOOL_NAMES,
    }
    assert must_keep.issubset(names)


def test_shrink_retry_keeps_core_runtime_tools() -> None:
    all_tools = get_tools(mcp_manager=None)
    shrunk = _shrink_tool_specs_for_retry(all_tools)
    names = {spec.name for spec in shrunk}
    assert "start_worker" in names
    assert _RUNTIME_PLAN_TOOL_NAMES.issubset(names)


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
    assert _RUNTIME_PLAN_TOOL_NAMES.issubset(names)
    assert "start_worker" in names
    assert "fs_read" in names
    assert "octo_opportunity_scan" in names
    assert "octo_self_queue_add" in names
    assert "execute_self_queue_item" in names
    assert "octo_self_queue_list" in names
    assert "octo_self_queue_take" in names
    assert "octo_self_queue_update" in names
    assert "repair_scheduled_tasks" in names
    assert "exec_run" in names
    assert "git_ops" in names
    assert "list_skills" in names
    assert "use_skill" in names
    assert "run_skill_script" in names
    assert "octo_restart_self" in names
    assert "octo_check_update" in names
    assert "octo_update_self" in names
    assert "test_run" not in names
    assert "exec_run" in {spec.name for spec in ctx["tool_resolution_report"].available_tools}
    assert _RUNTIME_PLAN_TOOL_NAMES.issubset(
        {spec.name for spec in ctx["tool_resolution_report"].available_tools}
    )
    assert "test_run" not in {spec.name for spec in ctx["tool_resolution_report"].available_tools}
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
    assert _RUNTIME_PLAN_TOOL_NAMES.issubset(names)


def test_get_octo_tools_keeps_runtime_plan_tools_under_tiny_budget(monkeypatch) -> None:
    class DummyOcto:
        mcp_manager = None

    monkeypatch.setenv("OCTOPAL_OCTO_MAX_INITIAL_TOOL_COUNT", "8")
    monkeypatch.setenv("OCTOPAL_OCTO_MAX_TOOL_COUNT", "8")
    monkeypatch.delenv("OCTOPAL_OCTO_DEFER_TOOL_LOADING", raising=False)

    tool_specs, _ctx = _get_octo_tools(DummyOcto(), 0)
    names = {spec.name for spec in tool_specs}

    assert _RUNTIME_PLAN_TOOL_NAMES.issubset(names)


def test_get_octo_tools_keeps_a2a_tools_when_enabled_despite_initial_budget(
    monkeypatch,
) -> None:
    class DummyOcto:
        mcp_manager = None
        runtime = SimpleNamespace(
            settings=SimpleNamespace(
                a2a=A2AConfig(
                    enabled=True,
                    peers={"alice": A2APeerConfig(name="Alice", token="secret")},
                )
            )
        )

    monkeypatch.setenv("OCTOPAL_OCTO_MAX_INITIAL_TOOL_COUNT", "8")

    tool_specs, _ctx = _get_octo_tools(DummyOcto(), 0)
    names = {spec.name for spec in tool_specs}

    assert "a2a_list_peers" in names
    assert "a2a_send_message" in names


def test_get_octo_tools_does_not_force_a2a_tools_when_disabled(monkeypatch) -> None:
    class DummyOcto:
        mcp_manager = None
        runtime = SimpleNamespace(settings=SimpleNamespace(a2a=A2AConfig(enabled=False)))

    monkeypatch.setenv("OCTOPAL_OCTO_MAX_INITIAL_TOOL_COUNT", "8")

    tool_specs, _ctx = _get_octo_tools(DummyOcto(), 0)
    names = {spec.name for spec in tool_specs}

    assert "a2a_list_peers" not in names
    assert "a2a_send_message" not in names


def test_route_or_reply_adds_a2a_context_prompt_when_enabled(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    class DummyMemory:
        async def add_message(self, role, content, metadata=None):
            return None

    class DummyOcto:
        store = object()
        canon = object()
        internal_progress_send = None
        is_ws_active = False
        mcp_manager = None
        runtime = SimpleNamespace(
            settings=SimpleNamespace(
                a2a=A2AConfig(
                    enabled=True,
                    peers={"alice": A2APeerConfig(name="Alice", token="secret")},
                )
            )
        )

        async def set_typing(self, chat_id: int, active: bool) -> None:
            return None

        async def set_thinking(self, active: bool) -> None:
            return None

        def peek_context_wakeup(self, chat_id: int) -> str:
            return ""

    async def fake_build_octo_prompt(**kwargs):
        return [
            Message(role="system", content="base prompt"),
            Message(role="user", content=str(kwargs["user_text"])),
        ]

    async def fake_build_plan(provider, messages, has_tools):
        return None

    captured: dict[str, object] = {}

    async def fake_complete_route_with_tools(**kwargs):
        captured["messages"] = kwargs["messages"]
        captured["tool_names"] = [tool.name for tool in kwargs["tool_specs"]]
        return "done"

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(router, "_build_plan", fake_build_plan)
    monkeypatch.setattr(router, "_complete_route_with_tools", fake_complete_route_with_tools)

    async def scenario() -> None:
        response = await route_or_reply(
            DummyOcto(),
            object(),
            DummyMemory(),
            "message Alice",
            123,
            "",
        )
        assert response == "done"

    asyncio.run(scenario())

    messages = captured["messages"]
    assert any("A2A interop is enabled" in str(message.content) for message in messages)
    assert any("alice: Alice" in str(message.content) for message in messages)
    assert "a2a_list_peers" in captured["tool_names"]
    assert "a2a_send_message" in captured["tool_names"]


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
            ToolSpec(
                name="octo_continue_from_control_route",
                description="continue",
                parameters={"type": "object", "properties": {}},
                permission="self_control",
                handler=lambda args, ctx: {"ok": True},
                is_async=True,
            ),
        ]

    class DummyOcto:
        mcp_manager = None

    monkeypatch.setattr(router, "get_tools", fake_get_tools)

    tools, _ctx = _get_worker_followup_tools(DummyOcto(), 123)

    assert {tool.name for tool in tools} == {
        "manage_canon",
        "get_worker_output_path",
        "fs_write",
        "octo_continue_from_control_route",
    }


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
                name="octo_continue_from_control_route",
                description="continue",
                parameters={"type": "object", "properties": {}},
                permission="self_control",
                handler=lambda args, ctx: {"ok": True},
                is_async=True,
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
    assert "mcp_agentmail_list_inboxes" not in {tool.name for tool in scheduled_octo_control_tools}
    assert "mcp_agentmail_list_inboxes" not in {tool.name for tool in internal_maintenance_tools}
    assert {"list_workers", "list_active_workers"}.issubset({tool.name for tool in scheduler_tools})
    assert {
        "list_workers",
        "list_active_workers",
        "manage_canon",
        "search_canon",
        "octo_continue_from_control_route",
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
                name="octo_continue_from_control_route",
                description="continue",
                parameters={"type": "object", "properties": {}},
                permission="self_control",
                handler=lambda args, ctx: {"ok": True},
                is_async=True,
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
    assert {tool.name for tool in tools} == {
        "manage_canon",
        "get_worker_output_path",
        "fs_write",
        "octo_continue_from_control_route",
    }


def test_worker_followup_fs_write_context_is_limited_to_durable_artifacts(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    class DummyOcto:
        mcp_manager = None

    tools, ctx = _get_worker_followup_tools(DummyOcto(), 123)
    fs_write_tool = next(tool for tool in tools if tool.name == "fs_write")

    assert ctx["base_dir"] == tmp_path.resolve()
    assert ctx["workspace_root"] == tmp_path.resolve()
    assert ctx["allowed_paths"] == ["reports", "artifacts"]
    assert ctx["restrict_to_allowed_paths"] is True

    assert fs_write_tool.handler({"path": "reports/out.md", "content": "ok"}, ctx) == "fs_write ok"
    assert (tmp_path / "reports" / "out.md").read_text(encoding="utf-8") == "ok"

    blocked = fs_write_tool.handler({"path": "mcp_servers.json", "content": "pwn"}, ctx)
    assert blocked.startswith("fs_write error:")
    assert "outside allowed paths" in blocked
    assert not (tmp_path / "mcp_servers.json").exists()


def test_control_route_continue_tool_runs_normal_route_and_sends(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(tmp_path))

    class DummyOcto:
        mcp_manager = None

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Done from normal route"

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    octo = DummyOcto()
    tools, ctx = _get_worker_followup_tools(octo, 123)
    continue_tool = next(tool for tool in tools if tool.name == "octo_continue_from_control_route")

    result = asyncio.run(
        continue_tool.handler(
            {
                "task": "Record the worker result in daily memory.",
                "context_summary": "Worker returned a useful note.",
            },
            ctx,
        )
    )

    payload = json.loads(result)
    assert payload["status"] == "continued"
    assert payload["delivered"] is True
    assert octo.sent == [(123, "Done from normal route")]
    assert octo.calls[0]["chat_id"] == 123
    assert "normal Octo conversation route" in str(octo.calls[0]["text"])
    assert "Record the worker result in daily memory." in str(octo.calls[0]["text"])
    kwargs = octo.calls[0]["kwargs"]
    assert kwargs["persist_to_memory"] is False
    assert kwargs["track_progress"] is True
    assert kwargs["background_delivery"] is True


def test_control_route_continue_tool_skips_stale_parent_turn() -> None:
    class DummyOcto:
        mcp_manager = None

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []
            self.epoch = 2

        def current_chat_turn_epoch(self, chat_id: int) -> int:
            return self.epoch

        def chat_turn_epoch_for_correlation(self, correlation_id: str | None, chat_id: int):
            return 1 if correlation_id == "old-turn" else None

        def is_chat_turn_epoch_current(self, chat_id: int, epoch: int | None) -> bool:
            return epoch == self.epoch

        def bind_correlation_to_chat_epoch(self, correlation_id, chat_id, epoch=None):
            return int(epoch or self.epoch)

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "should not run"

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    octo = DummyOcto()
    tools, ctx = _get_worker_followup_tools(octo, 123)
    ctx["correlation_id"] = "old-turn"
    ctx["chat_turn_epoch"] = 1
    continue_tool = next(tool for tool in tools if tool.name == "octo_continue_from_control_route")

    result = asyncio.run(
        continue_tool.handler(
            {
                "task": "Continue an old branch.",
                "context_summary": "This branch belongs to a stale turn.",
            },
            ctx,
        )
    )

    payload = json.loads(result)
    assert payload["status"] == "stale"
    assert payload["delivered"] is False
    assert octo.calls == []
    assert octo.sent == []


def test_control_route_continue_tool_drops_result_if_turn_becomes_stale() -> None:
    class DummyOcto:
        mcp_manager = None

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []
            self.epoch = 1
            self.bound: dict[str, int] = {"parent-turn": 1}

        def current_chat_turn_epoch(self, chat_id: int) -> int:
            return self.epoch

        def chat_turn_epoch_for_correlation(self, correlation_id: str | None, chat_id: int):
            return self.bound.get(str(correlation_id or ""))

        def is_chat_turn_epoch_current(self, chat_id: int, epoch: int | None) -> bool:
            return epoch == self.epoch

        def bind_correlation_to_chat_epoch(self, correlation_id, chat_id, epoch=None):
            self.bound[str(correlation_id or "")] = int(epoch or self.epoch)
            return self.bound[str(correlation_id or "")]

        def advance_chat_turn_epoch(self, chat_id: int) -> int:
            self.epoch += 1
            return self.epoch

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            self.advance_chat_turn_epoch(chat_id)
            return "Late stale result"

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    octo = DummyOcto()
    tools, ctx = _get_worker_followup_tools(octo, 123)
    ctx["correlation_id"] = "parent-turn"
    ctx["chat_turn_epoch"] = 1
    continue_tool = next(tool for tool in tools if tool.name == "octo_continue_from_control_route")

    result = asyncio.run(
        continue_tool.handler(
            {
                "task": "Continue a branch that will become stale.",
                "context_summary": "Another branch answers while this runs.",
            },
            ctx,
        )
    )

    payload = json.loads(result)
    assert payload["status"] == "stale"
    assert payload["delivered"] is False
    assert len(octo.calls) == 1
    assert octo.sent == []


def test_control_route_continue_tool_advances_epoch_after_delivery() -> None:
    class DummyOcto:
        mcp_manager = None

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []
            self.epoch = 1
            self.bound: dict[str, int] = {"parent-turn": 1}

        def current_chat_turn_epoch(self, chat_id: int) -> int:
            return self.epoch

        def chat_turn_epoch_for_correlation(self, correlation_id: str | None, chat_id: int):
            return self.bound.get(str(correlation_id or ""))

        def is_chat_turn_epoch_current(self, chat_id: int, epoch: int | None) -> bool:
            return epoch == self.epoch

        def bind_correlation_to_chat_epoch(self, correlation_id, chat_id, epoch=None):
            self.bound[str(correlation_id or "")] = int(epoch or self.epoch)
            return self.bound[str(correlation_id or "")]

        def advance_chat_turn_epoch(self, chat_id: int) -> int:
            self.epoch += 1
            return self.epoch

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Visible normal-route result"

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    octo = DummyOcto()
    tools, ctx = _get_worker_followup_tools(octo, 123)
    ctx["correlation_id"] = "parent-turn"
    ctx["chat_turn_epoch"] = 1
    continue_tool = next(tool for tool in tools if tool.name == "octo_continue_from_control_route")

    result = asyncio.run(
        continue_tool.handler(
            {
                "task": "Continue the current branch.",
                "context_summary": "No newer turn exists.",
            },
            ctx,
        )
    )

    payload = json.loads(result)
    assert payload["status"] == "continued"
    assert payload["delivered"] is True
    assert octo.sent == [(123, "Visible normal-route result")]
    assert octo.epoch == 2


def test_control_route_continue_tool_honors_scheduled_notify_never() -> None:
    class DummyOcto:
        mcp_manager = None

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Done from normal route"

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    octo = DummyOcto()
    tools, ctx = _get_scheduled_octo_control_tools(octo, 123)
    ctx["control_route_notify_user"] = "never"
    continue_tool = next(tool for tool in tools if tool.name == "octo_continue_from_control_route")

    result = asyncio.run(
        continue_tool.handler(
            {
                "task": "Run the normal-route continuation quietly.",
                "notify_user": True,
            },
            ctx,
        )
    )

    payload = json.loads(result)
    assert payload["status"] == "continued"
    assert payload["delivered"] is False
    assert payload["notify_user"] is False
    assert octo.sent == []
    handoff_text = str(octo.calls[0]["text"])
    assert "Delivery policy: complete this continuation silently." in handoff_text
    assert "Do not send messages, files, reactions, or user-facing updates." in handoff_text


def test_worker_followup_continue_tool_honors_notify_never(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    class DummyOcto:
        store = object()
        canon = object()
        facts = None
        reflection = None
        is_ws_active = False
        mcp_manager = None
        provider = object()

        def __init__(self) -> None:
            self.thinking_states: list[bool] = []
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []
            self.memory = object()

        async def set_thinking(self, active: bool) -> None:
            self.thinking_states.append(active)

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Visible normal-route result"

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_bootstrap_context_prompt(store, chat_id):
        return Message(role="system", content="bootstrap")

    async def fake_complete_route_with_tools(**kwargs):
        assert kwargs["ctx"]["control_route_notify_user"] == "never"
        continue_tool = next(
            tool for tool in kwargs["tool_specs"] if tool.name == "octo_continue_from_control_route"
        )
        result = await continue_tool.handler(
            {
                "task": "Continue quietly from the worker result.",
                "notify_user": True,
            },
            kwargs["ctx"],
        )
        payload = json.loads(result)
        assert payload["status"] == "continued"
        assert payload["delivered"] is False
        assert payload["notify_user"] is False
        return '{"user_response": null, "no_user_response": true, "actions_taken": [], "reason": "continued"}'

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(
        router, "build_bootstrap_context_prompt", fake_build_bootstrap_context_prompt
    )
    monkeypatch.setattr(router, "_complete_route_with_tools", fake_complete_route_with_tools)

    async def scenario() -> None:
        octo = DummyOcto()
        response = await route_worker_results_back_to_octo(
            octo,
            123,
            [
                (
                    "worker-1",
                    "write memory update",
                    WorkerResult(summary="needs normal-route memory write"),
                )
            ],
            notify_user="never",
        )
        assert response == "NO_USER_RESPONSE"
        assert octo.sent == []
        assert octo.calls
        assert "Delivery policy: complete this continuation silently." in str(octo.calls[0]["text"])
        assert octo.thinking_states == [True, False]

    asyncio.run(scenario())


def test_obvious_continuation_outcome_auto_continues_scheduled_control() -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def complete(self, messages, **kwargs):
            raise AssertionError("auto-continuation should not fall back to plain completion")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            assert self.calls == 1
            names = {tool["function"]["name"] for tool in tools}
            assert "octo_continue_from_control_route" in names
            assert "start_worker" not in names
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "type": "function",
                        "function": {
                            "name": "start_worker",
                            "arguments": '{"worker_id":"memory","task":"write update"}',
                        },
                    }
                ],
            }

    class DummyOcto:
        mcp_manager = None

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Normal route completed."

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    async def scenario() -> None:
        octo = DummyOcto()
        provider = DummyProvider()
        tools, ctx = _get_scheduled_octo_control_tools(octo, 123)
        ctx["control_route_notify_user"] = "never"

        reply = await _complete_route_with_tools(
            octo=octo,
            provider=provider,
            messages=[Message(role="user", content="scheduled task payload")],
            tool_specs=tools,
            ctx=ctx,
            internal_followup=False,
            user_text="Scheduled task payload: write update with a worker.",
            images=None,
            allow_tool_catalog_expansion=False,
        )

        assert reply == "SCHEDULED_TASK_DONE"
        assert provider.calls == 1
        assert octo.sent == []
        assert len(octo.calls) == 1
        handoff_text = str(octo.calls[0]["text"])
        assert "Complete the original turn through the normal Octo route." in handoff_text
        assert "attempted_tool: start_worker" in handoff_text
        assert "Scheduled task payload: write update with a worker." in handoff_text

    asyncio.run(scenario())


def test_tool_budget_exhaustion_continues_autonomously_instead_of_summarizing() -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def complete(self, messages, **kwargs):
            raise AssertionError("tool-budget fallback should not ask for a summary")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": f"call-{self.calls}",
                        "type": "function",
                        "function": {
                            "name": "dummy_tool",
                            "arguments": json.dumps({"idx": self.calls}),
                        },
                    }
                ],
            }

    class DummyOcto:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Finished after autonomous continuation."

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    def dummy_tool(args, ctx):
        return {"status": "ok", "idx": args.get("idx")}

    async def scenario() -> None:
        octo = DummyOcto()
        provider = DummyProvider()
        reply = await _complete_route_with_tools(
            octo=octo,
            provider=provider,
            messages=[Message(role="user", content="finish durable plan")],
            tool_specs=[
                ToolSpec(
                    name="dummy_tool",
                    description="dummy",
                    parameters={
                        "type": "object",
                        "properties": {"idx": {"type": "integer"}},
                        "additionalProperties": False,
                    },
                    permission="exec",
                    handler=dummy_tool,
                )
            ],
            ctx={"octo": octo, "chat_id": 123},
            internal_followup=False,
            user_text="finish durable plan",
            images=None,
            allow_tool_catalog_expansion=False,
        )

        assert reply == "NO_USER_RESPONSE"
        assert provider.calls == 10
        assert len(octo.calls) == 1
        handoff_text = str(octo.calls[0]["text"])
        assert "Runtime continuation after tool budget exhaustion" in handoff_text
        assert "Do not ask the user to say continue" in handoff_text
        assert octo.sent == [(123, "Finished after autonomous continuation.")]

    asyncio.run(scenario())


def test_tool_budget_continuation_honors_notify_never() -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def complete(self, messages, **kwargs):
            raise AssertionError("silent continuation should not fall back to user text")

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": f"call-{self.calls}",
                        "type": "function",
                        "function": {
                            "name": "dummy_tool",
                            "arguments": json.dumps({"idx": self.calls}),
                        },
                    }
                ],
            }

    class DummyOcto:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "This should stay silent."

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    async def scenario() -> None:
        octo = DummyOcto()
        provider = DummyProvider()
        reply = await _complete_route_with_tools(
            octo=octo,
            provider=provider,
            messages=[Message(role="user", content="quiet scheduled work")],
            tool_specs=[
                ToolSpec(
                    name="dummy_tool",
                    description="dummy",
                    parameters={
                        "type": "object",
                        "properties": {"idx": {"type": "integer"}},
                        "additionalProperties": False,
                    },
                    permission="exec",
                    handler=lambda args, ctx: {"status": "ok", "idx": args.get("idx")},
                )
            ],
            ctx={"octo": octo, "chat_id": 123, "control_route_notify_user": "never"},
            internal_followup=False,
            user_text="quiet scheduled work",
            images=None,
            allow_tool_catalog_expansion=False,
        )

        assert reply == "NO_USER_RESPONSE"
        assert provider.calls == 10
        assert octo.sent == []
        handoff_text = str(octo.calls[0]["text"])
        assert "Delivery policy: complete this continuation silently." in handoff_text

    asyncio.run(scenario())


def test_tool_budget_continuation_without_delivery_falls_back_instead_of_preview() -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.fallback_seen = False

        async def complete(self, messages, **kwargs):
            self.fallback_seen = any(
                "Tool execution reached the route budget and autonomous continuation was unavailable"
                in str(message.get("content", ""))
                for message in messages
            )
            assert self.fallback_seen
            return "Continuation finished, but delivery needs a normal final response."

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.calls += 1
            return {
                "content": "",
                "tool_calls": [
                    {
                        "id": f"call-{self.calls}",
                        "type": "function",
                        "function": {
                            "name": "dummy_tool",
                            "arguments": json.dumps({"idx": self.calls}),
                        },
                    }
                ],
            }

    class DummyOcto:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "x" * 500

    async def scenario() -> None:
        octo = DummyOcto()
        provider = DummyProvider()
        reply = await _complete_route_with_tools(
            octo=octo,
            provider=provider,
            messages=[Message(role="user", content="finish but cannot send")],
            tool_specs=[
                ToolSpec(
                    name="dummy_tool",
                    description="dummy",
                    parameters={
                        "type": "object",
                        "properties": {"idx": {"type": "integer"}},
                        "additionalProperties": False,
                    },
                    permission="exec",
                    handler=lambda args, ctx: {"status": "ok", "idx": args.get("idx")},
                )
            ],
            ctx={"octo": octo, "chat_id": 123},
            internal_followup=False,
            user_text="finish but cannot send",
            images=None,
            allow_tool_catalog_expansion=False,
        )

        assert provider.calls == 10
        assert provider.fallback_seen is True
        assert reply == "Continuation finished, but delivery needs a normal final response."
        assert reply != "x" * 240

    asyncio.run(scenario())


def test_silent_control_route_suppresses_normal_route_reply_delivery() -> None:
    token = suppress_user_delivery()
    try:
        decision = resolve_user_delivery("Normal route completed.")
    finally:
        restore_user_delivery(token)

    assert decision.user_visible is False
    assert decision.reason == "delivery_suppressed"


def test_send_file_to_user_respects_suppressed_delivery(tmp_path: Path) -> None:
    class DummyOcto:
        async def internal_send_file(
            self, chat_id: int, path: str, caption: str | None = None
        ) -> None:
            raise AssertionError("file delivery should be blocked when user delivery is suppressed")

    token = suppress_user_delivery()
    try:
        result = asyncio.run(
            send_file_to_user(
                {"path": "reports/out.txt"},
                {
                    "octo": DummyOcto(),
                    "chat_id": 123,
                    "base_dir": tmp_path,
                },
            )
        )
    finally:
        restore_user_delivery(token)

    payload = json.loads(result)
    assert payload["status"] == "error"
    assert payload["message"] == "user delivery is suppressed for this continuation"


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
            ToolSpec(
                name="octo_continue_from_control_route",
                description="continue",
                parameters={"type": "object", "properties": {}},
                permission="self_control",
                handler=lambda args, ctx: {"ok": True},
                is_async=True,
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
            "octo_continue_from_control_route",
        }
        assert octo.thinking_states == [True, False]

    asyncio.run(scenario())


def test_normalize_worker_followup_reply_uses_structured_user_response() -> None:
    raw = """
    {
      "user_response": "Briefing is ready.",
      "no_user_response": false,
      "actions_taken": [{"type": "get_worker_output_path", "summary": "checked output"}],
      "reason": "worker completed"
    }
    """
    assert _normalize_worker_followup_reply(raw) == "Briefing is ready."


def test_normalize_worker_followup_reply_strips_noisy_user_visible_wrapper() -> None:
    raw = (
        "I checked internal worker state and should only show the marked part.\n\n"
        "<user_visible>Briefing is ready.</user_visible>"
    )
    assert _normalize_worker_followup_reply(raw) == "Briefing is ready."


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


def test_normalize_worker_followup_reply_honors_structured_no_response_over_text() -> None:
    raw = """
    {
      "user_response": "I'm in bounded worker-result follow-up mode and can't modify the schedule from here. I'll do this on the next turn.",
      "no_user_response": true,
      "actions_taken": [],
      "reason": "needs orchestration"
    }
    """
    assert _normalize_worker_followup_reply(raw) == "NO_USER_RESPONSE"


def test_normalize_worker_followup_reply_uses_structured_response_without_phrase_guessing() -> None:
    raw = """
    {
      "user_response": "I don't have the A2A messaging tools available in my current tool set. I need to send the message from my full orchestration context. I will send it once I am back in full mode.",
      "no_user_response": false,
      "actions_taken": [],
      "reason": "needs orchestration"
    }
    """
    assert _normalize_worker_followup_reply(raw) == (
        "I don't have the A2A messaging tools available in my current tool set. "
        "I need to send the message from my full orchestration context. "
        "I will send it once I am back in full mode."
    )


def test_worker_followup_autonomously_continues_nonfinal_runtime_action(monkeypatch) -> None:
    import octopal.runtime.octo.router as router

    class DummyProvider:
        def __init__(self) -> None:
            self.verifier_seen = False

        async def complete(self, messages, **kwargs):
            self.verifier_seen = any(
                "Classify whether a worker-result follow-up output may be delivered"
                in str(message.get("content", ""))
                for message in messages
            )
            assert self.verifier_seen
            return (
                '{"verdict":"requires_continuation","confidence":0.91,'
                '"reason":"draft describes pending runtime work instead of a final result"}'
            )

        async def complete_stream(self, messages, *, on_partial, **kwargs):
            raise AssertionError("streaming should not be used in this test")

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
            self.calls: list[dict[str, object]] = []
            self.sent: list[tuple[int, str]] = []

        async def set_thinking(self, active: bool) -> None:
            self.thinking_states.append(active)

        async def handle_message(self, text: str, chat_id: int, **kwargs):
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return "Done from normal route."

        async def internal_send(self, chat_id: int, text: str) -> None:
            self.sent.append((chat_id, text))

    async def fake_build_octo_prompt(**kwargs):
        return [Message(role="user", content=str(kwargs["user_text"]))]

    async def fake_build_bootstrap_context_prompt(store, chat_id):
        return Message(role="system", content="bootstrap")

    async def fake_complete_route_with_tools(**kwargs):
        return """
        {
          "user_response": "I need to continue this from the broader runtime path.",
          "no_user_response": false,
          "actions_taken": [],
          "reason": "needs more runtime action"
        }
        """

    monkeypatch.setattr(router, "build_octo_prompt", fake_build_octo_prompt)
    monkeypatch.setattr(
        router, "build_bootstrap_context_prompt", fake_build_bootstrap_context_prompt
    )
    monkeypatch.setattr(router, "_complete_route_with_tools", fake_complete_route_with_tools)

    async def scenario() -> None:
        octo = DummyOcto()
        response = await route_worker_results_back_to_octo(
            octo,
            123,
            [
                (
                    "worker-1",
                    "send the final status via the normal runtime",
                    WorkerResult(summary="needs normal-route continuation"),
                )
            ],
        )

        assert response == "NO_USER_RESPONSE"
        assert octo.provider.verifier_seen is True
        assert len(octo.calls) == 1
        assert len(octo.sent) == 1
        assert octo.sent[0] == (123, "Done from normal route.")
        handoff_text = str(octo.calls[0]["text"])
        assert "Continue the original user request autonomously" in handoff_text
        assert "Rejected non-final follow-up draft" in handoff_text
        assert octo.thinking_states == [True, False]

    asyncio.run(scenario())


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


def test_image_fallback_without_saved_paths_uses_channel_neutral_directory(
    monkeypatch, tmp_path
) -> None:
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
                return {"content": "I will take care of that.", "tool_calls": []}
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
            return {"content": "Checked: ok.", "tool_calls": []}

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
            "check status",
            123,
            "",
        )
        assert response == "Checked: ok."
        assert provider.calls == 3
        assert provider.verifier_seen is True
        assert provider.retry_prompt_seen is True

    asyncio.run(scenario())


def test_action_state_retry_uses_verifier_instead_of_keyword_heuristic() -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.verifier_seen = False

        async def complete(self, messages, **kwargs):
            self.verifier_seen = any(
                "Classify whether the draft assistant response is safe to deliver"
                in str(getattr(message, "content", "") or message.get("content", ""))
                for message in messages
            )
            return '{"verdict":"requires_runtime_action_state","confidence":0.91,"reason":"draft needs action state"}'

    async def scenario() -> None:
        provider = DummyProvider()
        assert await _needs_action_or_blocked_retry(
            provider=provider,
            messages=[Message(role="user", content="install mcp, then activate it")],
            candidate="`uvx` is available. Installing MiniMax MCP...",
        )
        assert provider.verifier_seen is True

    asyncio.run(scenario())


def test_route_forces_pending_runtime_plan_step_to_runtime_state(monkeypatch) -> None:
    plan_result = {
        "status": "ok",
        "run_id": "plan-1",
        "snapshot": {
            "run": {"id": "plan-1", "status": "planned", "current_step_id": "step-1"},
            "steps": [
                {
                    "run_id": "plan-1",
                    "step_id": "step-1",
                    "kind": "worker",
                    "title": "Run worker",
                    "status": "pending",
                }
            ],
            "next_step": {
                "run_id": "plan-1",
                "step_id": "step-1",
                "kind": "worker",
                "title": "Run worker",
                "status": "pending",
            },
        },
    }

    worker_result = {
        "status": "started",
        "worker_id": "worker-1",
        "run_id": "worker-1",
        "followup_required": True,
        "plan_binding": {
            "status": "ok",
            "run_id": "plan-1",
            "step_id": "step-1",
            "worker_run_id": "worker-1",
        },
    }

    class DummyProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.retry_prompt_seen = False

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
                            "id": "call-plan",
                            "type": "function",
                            "function": {"name": "plan_create", "arguments": "{}"},
                        }
                    ],
                }
            if self.calls == 2:
                return {"content": "Plan is updated.", "tool_calls": []}
            if self.calls == 3:
                self.retry_prompt_seen = any(
                    "Runtime state still contains an actionable plan step"
                    in str(getattr(message, "content", ""))
                    for message in messages
                )
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-worker",
                            "type": "function",
                            "function": {
                                "name": "start_worker",
                                "arguments": json.dumps(
                                    {
                                        "plan_run_id": "plan-1",
                                        "plan_step_id": "step-1",
                                    }
                                ),
                            },
                        }
                    ],
                }
            return {"content": "Worker is recorded in runtime state.", "tool_calls": []}

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
        def plan_create(args, ctx):
            return json.dumps(plan_result)

        def start_worker(args, ctx):
            assert args["plan_run_id"] == "plan-1"
            assert args["plan_step_id"] == "step-1"
            return json.dumps(worker_result)

        tools = [
            ToolSpec(
                name="plan_create",
                description="Create plan",
                parameters={"type": "object", "properties": {}, "additionalProperties": False},
                permission="self_control",
                handler=plan_create,
            ),
            ToolSpec(
                name="start_worker",
                description="Start worker",
                parameters={"type": "object", "properties": {}, "additionalProperties": True},
                permission="worker_manage",
                handler=start_worker,
            ),
        ]
        return tools, {"octo": octo, "chat_id": chat_id}

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
            "do the task",
            123,
            "",
        )
        assert response == "Worker is recorded in runtime state."
        assert provider.calls == 4
        assert provider.retry_prompt_seen is True
        assert octo.followup_marked == 1

    asyncio.run(scenario())


def test_parallel_worker_launch_resolves_bound_runtime_action_contract() -> None:
    contracts = [
        RuntimeActionContract(
            run_id="plan-1",
            step_id="collect",
            kind="worker",
            title="Collect evidence",
        ),
        RuntimeActionContract(
            run_id="plan-1",
            step_id="summarize",
            kind="worker",
            title="Summarize evidence",
        ),
    ]
    parallel_result = {
        "status": "partial",
        "launches": [
            {
                "status": "started",
                "worker_id": "worker-1",
                "plan_binding": {
                    "status": "ok",
                    "run_id": "plan-1",
                    "step_id": "collect",
                    "worker_run_id": "worker-1",
                },
            },
            {
                "status": "error",
                "plan_binding": {
                    "status": "not_found",
                    "run_id": "plan-1",
                    "step_id": "missing",
                },
            },
        ],
    }

    remaining = _update_runtime_action_contracts(
        contracts,
        tool_name="start_workers_parallel",
        tool_result=json.dumps(parallel_result),
    )

    assert remaining == [contracts[1]]


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
    assert '"status":"ok"' in tool_summary["content"]


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


def test_finalize_response_revises_runtime_plan_state_leak() -> None:
    class DummyProvider:
        def __init__(self) -> None:
            self.review_seen = False

        async def complete(self, messages, **kwargs):
            self.review_seen = any(
                "Review a draft user-facing response that was generated after runtime-state tools"
                in str(message.get("content", ""))
                for message in messages
            )
            assert self.review_seen
            return json.dumps(
                {
                    "verdict": "revised",
                    "response": (
                        "Правда про пост: он есть, опубликован, виден и verified. "
                        "Я неправильно прочитала worker output и составила ложный success, "
                        "но сам пост настоящий."
                    ),
                    "confidence": 0.93,
                    "reason": "removed runtime plan metadata while preserving user facts",
                }
            )

    async def scenario() -> None:
        provider = DummyProvider()
        result = await _finalize_response(
            provider,
            [
                Message(role="user", content="проверь пост"),
                {
                    "role": "tool",
                    "name": "plan_update_step",
                    "content": '{"run_id":"plan-13cfadc5","status":"completed","next_step":null}',
                },
            ],
            (
                "Plan plan-13cfadc5 is completed — completed_at: "
                "2026-06-15T22:35:52.287508Z, next_step: null. Now the honest user-facing report.\n\n"
                "Правда про пост: он есть, опубликован, виден, verified. "
                "Я неправильно прочитала worker output и составила ложный success — "
                "но сам пост настоящий."
            ),
            internal_followup=False,
        )

        assert provider.review_seen is True
        assert "Plan plan-" not in result
        assert "completed_at" not in result
        assert "next_step" not in result
        assert "пост настоящий" in result

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
            return {"content": "Checking now; I will follow up with the result.", "tool_calls": []}

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
        assert response == "Checking now; I will follow up with the result."
        assert octo.followup_marked == 1

    asyncio.run(scenario())


def test_route_retries_when_response_delegates_recoverable_choices(monkeypatch) -> None:
    tool_calls_seen: list[dict] = []

    recovery_tool = ToolSpec(
        name="inspect_worker_state",
        description="Inspect worker state and optionally choose a recovery strategy.",
        parameters={
            "type": "object",
            "properties": {"strategy": {"type": "string"}},
            "additionalProperties": False,
        },
        permission="self_control",
        handler=lambda args, ctx: tool_calls_seen.append(dict(args or {}))
        or {"status": "ok", "strategy": (args or {}).get("strategy")},
    )

    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0
            self.verifier_seen = False

        async def complete(self, messages, **kwargs):
            prompt = "\n".join(str(message.get("content", "")) for message in messages)
            assert "improperly delegates recoverable execution choices" in prompt
            self.verifier_seen = True
            return json.dumps(
                {
                    "verdict": "requires_autonomous_recovery",
                    "confidence": 0.92,
                    "reason": "draft asks user to pick among safe retry paths",
                }
            )

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
            if self.tool_calls == 1:
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {
                                "name": "inspect_worker_state",
                                "arguments": "{}",
                            },
                        }
                    ],
                }
            if self.tool_calls == 2:
                return {
                    "content": (
                        "Диагноз готов, fix — не сработал.\n\n"
                        "Три варианта на следующий заход:\n"
                        "1. Увеличить worker timeout\n"
                        "2. Разбить на 5 single-step воркеров\n"
                        "3. Самой стучаться в API\n\n"
                        "Скажи цифру — 1, 2 или 3 — и я пойду в эту сторону."
                    ),
                    "tool_calls": [],
                }
            if self.tool_calls == 3:
                assert any(
                    "delegated recoverable execution choices"
                    in str(
                        message.get("content", "")
                        if isinstance(message, dict)
                        else getattr(message, "content", "")
                    )
                    for message in messages
                )
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-2",
                            "type": "function",
                            "function": {
                                "name": "inspect_worker_state",
                                "arguments": '{"strategy":"split_worker"}',
                            },
                        }
                    ],
                }
            return {
                "content": "Я выбрала безопасный следующий шаг сама и запустила split-worker recovery.",
                "tool_calls": [],
            }

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
        return [recovery_tool], {
            "octo": octo,
            "chat_id": chat_id,
            "active_tool_specs": [recovery_tool],
            "all_tool_specs": [recovery_tool],
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
            "проверь и почини",
            123,
            "",
        )

        assert provider.verifier_seen is True
        assert provider.tool_calls == 4
        assert tool_calls_seen == [{}, {"strategy": "split_worker"}]
        assert "Скажи цифру" not in response
        assert (
            response == "Я выбрала безопасный следующий шаг сама и запустила split-worker recovery."
        )

    asyncio.run(scenario())


def test_route_retries_when_execution_plan_delegates_choices_before_tools(monkeypatch) -> None:
    tool_calls_seen: list[dict] = []

    repair_tool = ToolSpec(
        name="repair_pipeline",
        description="Repair a recoverable pipeline issue.",
        parameters={
            "type": "object",
            "properties": {"target": {"type": "string"}},
            "additionalProperties": False,
        },
        permission="self_control",
        handler=lambda args, ctx: tool_calls_seen.append(dict(args or {}))
        or {"status": "ok", "target": (args or {}).get("target")},
    )

    class DummyProvider:
        def __init__(self) -> None:
            self.tool_calls = 0
            self.verifier_seen = False

        async def complete(self, messages, **kwargs):
            prompt = "\n".join(str(message.get("content", "")) for message in messages)
            assert "improperly delegates recoverable execution choices" in prompt
            assert "<execution_plan>" in prompt
            self.verifier_seen = True
            return json.dumps(
                {
                    "verdict": "requires_autonomous_recovery",
                    "confidence": 0.93,
                    "reason": "draft asks for a choice instead of executing the plan",
                }
            )

        async def complete_with_tools(self, messages, *, tools, tool_choice="auto", **kwargs):
            self.tool_calls += 1
            if self.tool_calls == 1:
                return {
                    "content": (
                        "Дальше два вопроса:\n"
                        "1. Делать publish сейчас через прямой curl?\n"
                        "2. Или сначала починить pipeline?\n\n"
                        "Скажи 1 или 2."
                    ),
                    "tool_calls": [],
                }
            if self.tool_calls == 2:
                assert any(
                    "instead of following the active execution plan"
                    in str(
                        message.get("content", "")
                        if isinstance(message, dict)
                        else getattr(message, "content", "")
                    )
                    for message in messages
                )
                return {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {
                                "name": "repair_pipeline",
                                "arguments": '{"target":"worker_template"}',
                            },
                        }
                    ],
                }
            return {"content": "Починила pipeline и продолжила publish path.", "tool_calls": []}

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
        assert has_tools is True
        return {
            "mode": "execute",
            "steps": [
                "Verify why the worker cannot see the Moltbook key",
                "Repair the worker template or mark a concrete blocker",
            ],
            "response": "This planner response must not become the final answer.",
        }

    def fake_get_octo_tools(octo, chat_id):
        return [repair_tool], {
            "octo": octo,
            "chat_id": chat_id,
            "active_tool_specs": [repair_tool],
            "all_tool_specs": [repair_tool],
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
            "почему нет ключа?",
            123,
            "",
        )

        assert provider.verifier_seen is True
        assert provider.tool_calls == 3
        assert tool_calls_seen == [{"target": "worker_template"}]
        assert "Скажи 1 или 2" not in response
        assert response == "Починила pipeline и продолжила publish path."

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
