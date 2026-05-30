from __future__ import annotations

from octopal.runtime.workers.agent_worker import (
    _build_worker_completion_protocol_prompt,
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


def test_worker_tool_inventory_omits_duplicate_descriptions_by_default(monkeypatch) -> None:
    monkeypatch.delenv("OCTOPAL_WORKER_PROMPT_TOOL_DESCRIPTIONS", raising=False)
    tool = ToolSpec(
        name="web_search",
        description="Search the web with a deliberately long duplicate schema description.",
        parameters={"type": "object"},
        permission="network",
        handler=lambda args, ctx: {"ok": True},
    )

    inventory = _build_worker_tool_inventory_prompt([tool])

    assert inventory == "- web_search"
    assert "duplicate schema description" not in inventory


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

    assert len(prompt) < 420
    assert 'type="result"' in prompt
    assert "summary" in prompt
    assert "output/questions" in prompt
    assert "request_instruction" in prompt
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
