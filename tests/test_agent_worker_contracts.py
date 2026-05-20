from __future__ import annotations

from octopal.runtime.workers.agent_worker import (
    _build_worker_file_write_prompt,
    _build_worker_skill_usage_prompt,
    _build_worker_tool_inventory_prompt,
    _force_tool_choice,
    _fs_write_completion_missing,
    _record_worker_llm_context_snapshot,
    _record_worker_tool_result_context,
    _task_requires_workspace_write,
)
from octopal.tools.registry import ToolSpec


def test_workspace_write_task_detection_requires_write_intent_and_file_hint() -> None:
    assert _task_requires_workspace_write(
        "Create a short markdown report at experiments/qa/marker-worker-report.md"
    )
    assert not _task_requires_workspace_write("Summarize the latest provider news")


def test_fs_write_completion_missing_requires_available_but_unused_tool() -> None:
    task = "Write the report to experiments/qa/marker-worker-report.md"

    assert _fs_write_completion_missing(task, ["fs_read", "fs_write"], [])
    assert not _fs_write_completion_missing(task, ["fs_read", "fs_write"], ["fs_write"])
    assert not _fs_write_completion_missing(task, ["web_search"], [])


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
