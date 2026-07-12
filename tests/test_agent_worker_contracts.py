from __future__ import annotations

from octopal.runtime.workers.agent_worker import (
    _build_worker_completion_protocol_prompt,
    _build_worker_context_manifest,
    _build_worker_file_write_prompt,
    _build_worker_skill_usage_prompt,
    _build_worker_task_prompt,
    _build_worker_tool_inventory_prompt,
    _force_tool_choice,
    _make_request_instruction_tool,
    _record_worker_llm_context_snapshot,
    _record_worker_tool_result_context,
    _required_tool_call_missing,
    _tool_schema_chars,
)
from octopal.runtime.workers.contracts import WorkerSpec
from octopal.tools.registry import ToolSpec


def test_required_tool_call_missing_uses_explicit_contract() -> None:
    assert _required_tool_call_missing(["fs_write"], [], "fs_write")
    assert not _required_tool_call_missing(["fs_write"], ["fs_write"], "fs_write")
    assert not _required_tool_call_missing([], [], "fs_write")


def test_force_tool_choice_uses_openai_function_shape() -> None:
    assert _force_tool_choice("fs_write") == {
        "type": "function",
        "function": {"name": "fs_write"},
    }


def test_worker_tool_inventory_includes_compact_descriptions_by_default(monkeypatch) -> None:
    monkeypatch.delenv("OCTOPAL_WORKER_PROMPT_TOOL_DESCRIPTIONS", raising=False)
    tool = ToolSpec(
        name="web_search",
        description=(
            "Search the web with a deliberately long duplicate schema description "
            + "that should be shortened for worker prompt compactness. " * 5
        ),
        parameters={"type": "object"},
        permission="network",
        handler=lambda args, ctx: {"ok": True},
    )

    inventory = _build_worker_tool_inventory_prompt([tool])

    assert inventory.startswith("- web_search: Search the web")
    assert "duplicate schema description" in inventory
    assert len(inventory) < 160
    assert inventory.endswith("...")


def test_worker_tool_inventory_can_omit_descriptions(monkeypatch) -> None:
    monkeypatch.setenv("OCTOPAL_WORKER_PROMPT_TOOL_DESCRIPTIONS", "false")
    tool = ToolSpec(
        name="web_search",
        description="Search the web with a schema description.",
        parameters={"type": "object"},
        permission="network",
        handler=lambda args, ctx: {"ok": True},
    )

    inventory = _build_worker_tool_inventory_prompt([tool])

    assert inventory == "- web_search"
    assert "schema description" not in inventory


def test_worker_file_write_prompt_requires_fs_write_tool() -> None:
    read_tool = ToolSpec(
        name="fs_read",
        description="Read a file.",
        parameters={"type": "object"},
        permission="read",
        handler=lambda args, ctx: {"ok": True},
    )
    write_tool = ToolSpec(
        name="fs_write",
        description="Write a file.",
        parameters={"type": "object"},
        permission="write",
        handler=lambda args, ctx: {"ok": True},
    )

    assert _build_worker_file_write_prompt([read_tool]) == ""
    prompt = _build_worker_file_write_prompt([read_tool, write_tool])
    assert "fs_write" in prompt
    assert "returns successfully" in prompt


def test_worker_skill_usage_prompt_requires_skill_tools() -> None:
    read_tool = ToolSpec(
        name="fs_read",
        description="Read a file.",
        parameters={"type": "object"},
        permission="read",
        handler=lambda args, ctx: {"ok": True},
    )
    use_skill_tool = ToolSpec(
        name="use_skill",
        description="Read a skill.",
        parameters={"type": "object"},
        permission="read",
        handler=lambda args, ctx: {"ok": True},
    )

    assert _build_worker_skill_usage_prompt([read_tool]) == ""
    prompt = _build_worker_skill_usage_prompt([read_tool, use_skill_tool])
    assert "Skill usage:" in prompt
    assert "use_skill" in prompt


def test_worker_task_prompt_omits_empty_inputs_and_keeps_unicode_compact() -> None:
    assert _build_worker_task_prompt("Summarize the news", {}) == "Task: Summarize the news"

    prompt = _build_worker_task_prompt(
        "Answer",
        {"query": "Привет", "limit": 3},
    )

    assert prompt == 'Task: Answer\n\nInputs JSON: {"query":"Привет","limit":3}'
    assert "\\u041f" not in prompt
    assert "\n  " not in prompt


def test_worker_completion_protocol_keeps_required_contract_concise() -> None:
    prompt = _build_worker_completion_protocol_prompt()

    assert len(prompt) < 520
    assert 'type="result"' in prompt
    assert 'status="completed" or "failed"' in prompt
    assert "summary" in prompt
    assert "output" in prompt
    assert "questions" in prompt
    assert "request_instruction" in prompt
    assert "awaiting_instruction is runtime state" in prompt
    assert "transport/debug/auth" in prompt


def test_request_instruction_tool_schema_stays_compact() -> None:
    class _Worker:
        async def request_instruction(self, **_kwargs):
            return {}

    tool = _make_request_instruction_tool(_Worker())

    assert _tool_schema_chars([tool]) < 760
    assert "blocking guidance" in tool.description
    assert "question" in tool.parameters["required"]
    assert tool.parameters["properties"]["target"]["enum"] == ["octo", "parent"]


def test_worker_context_telemetry_records_prompt_and_tool_result_growth() -> None:
    tool = ToolSpec(
        name="web_fetch",
        description="Fetch web content.",
        parameters={"type": "object", "properties": {"url": {"type": "string"}}},
        permission="network",
        handler=lambda args, ctx: {"ok": True},
    )
    telemetry = {
        "context": {
            "tool_schema_chars": 0,
            "llm_input_chars_total": 0,
            "llm_input_chars_peak": 0,
            "message_count_peak": 0,
            "tool_result_raw_chars_total": 0,
            "tool_result_rendered_chars_total": 0,
            "tool_result_rendered_chars_by_tool": {},
            "tool_result_truncated_chars_total": 0,
            "llm_calls": [],
        }
    }
    messages = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "task"},
    ]

    _record_worker_llm_context_snapshot(telemetry, messages=messages, tools=[tool], step=0)
    _record_worker_tool_result_context(
        telemetry,
        tool_name="web_fetch",
        raw_result={"content": "x" * 100},
        rendered_text="x" * 20,
        was_compacted=True,
    )

    context = telemetry["context"]
    assert context["llm_input_chars_total"] > 0
    assert context["llm_input_chars_peak"] == context["llm_calls"][0]["input_chars"]
    assert context["message_count_peak"] == 2
    assert context["tool_result_raw_chars_total"] > context["tool_result_rendered_chars_total"]
    assert context["tool_result_rendered_chars_by_tool"]["web_fetch"] == 20
    assert context["tool_result_truncated_chars_total"] > 0


def test_worker_context_manifest_records_selection_without_prompt_content() -> None:
    tool = ToolSpec(
        name="web_fetch",
        description="Fetch web content.",
        parameters={"type": "object", "properties": {"url": {"type": "string"}}},
        permission="network",
        handler=lambda args, ctx: {"ok": True},
    )
    spec = WorkerSpec(
        id="worker-1",
        template_id="research",
        task="Secret task text must not appear in telemetry",
        inputs={"private": "secret input"},
        system_prompt="Research carefully.",
        available_tools=["web_fetch", "missing_tool"],
        granted_capabilities=[],
        timeout_seconds=30,
        max_thinking_steps=4,
        run_id="run-1",
        effective_permissions=["network"],
        allowed_paths=["reports"],
    )

    manifest = _build_worker_context_manifest(
        spec=spec,
        tools=[tool],
        prompt_sections={"system": "system text", "task": spec.task},
    )

    assert manifest["version"] == 1
    assert manifest["task"]["run_id"] == "run-1"
    assert manifest["tools"]["active_names"] == ["web_fetch"]
    assert manifest["tools"]["unavailable_requested_names"] == ["missing_tool"]
    assert manifest["tools"]["schema_chars_by_tool"]["web_fetch"] > 0
    assert manifest["prompt_sections_chars"]["task"] == len(spec.task)
    assert manifest["policy"]["allowed_path_count"] == 1
    assert "Secret task text" not in str(manifest)
    assert "secret input" not in str(manifest)
