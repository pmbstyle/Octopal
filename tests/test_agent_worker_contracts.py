from __future__ import annotations

from octopal.runtime.workers.agent_worker import (
    _force_tool_choice,
    _fs_write_completion_missing,
    _task_requires_workspace_write,
)


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
