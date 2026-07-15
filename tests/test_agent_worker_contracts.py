from __future__ import annotations

import asyncio

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.store.models import (
    ProceduralRecipeContext,
    procedural_recipe_definition_fingerprint,
)
from octopal.runtime.workers.agent_worker import (
    _build_procedural_recipe_prompt,
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
    _with_programmatic_read_proxies,
)
from octopal.runtime.workers.contracts import WorkerSpec
from octopal.tools.metadata import ProgrammaticReadContract, ToolMetadata
from octopal.tools.registry import ToolSpec
from octopal.worker_sdk.worker import Worker


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


def test_programmatic_read_proxy_keeps_normal_tool_shape_and_calls_host_bridge(
    monkeypatch,
) -> None:
    worker = Worker(
        spec=WorkerSpec(
            id="worker-1",
            task="Research",
            inputs={},
            system_prompt="Research",
            available_tools=["web_search"],
            granted_capabilities=[],
            timeout_seconds=30,
            max_thinking_steps=3,
            effective_permissions=["network"],
            programmatic_read_call_budget=1,
        )
    )
    calls: list[dict] = []

    async def _fake_batch(batch: list[dict]) -> dict:
        calls.extend(batch)
        return {
            "results": [
                {
                    "call_id": batch[0]["call_id"],
                    "tool_name": "web_search",
                    "status": "completed",
                    "value": {"ok": True, "results": []},
                }
            ]
        }

    monkeypatch.setattr(worker, "programmatic_read_batch", _fake_batch)
    tool = ToolSpec(
        name="web_search",
        description="Search",
        parameters={"type": "object"},
        permission="network",
        handler=lambda args, ctx: "local handler must not run",
        metadata=ToolMetadata(
            category="web",
            read_only=True,
            programmatic_read=ProgrammaticReadContract(
                idempotent=True,
                max_parallel_calls=2,
                result_shape="json_object",
                max_result_bytes=64_000,
            ),
        ),
    )

    proxied = _with_programmatic_read_proxies([tool], worker)[0]
    result = asyncio.run(proxied.handler({"query": "Octopal"}, {}))

    assert proxied.name == tool.name
    assert proxied.parameters == tool.parameters
    assert proxied.metadata == tool.metadata
    assert result == {"ok": True, "results": []}
    assert calls[0]["tool_name"] == "web_search"
    assert calls[0]["arguments"] == {"query": "Octopal"}


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
        memory_influence_ids=["memory_fact:fact-1", "memory_entry:entry-2"],
        llm_config=LLMConfig(provider_id="openrouter", model="resolved-model"),
    )

    manifest = _build_worker_context_manifest(
        spec=spec,
        tools=[tool],
        prompt_sections={"system": "system text", "task": spec.task},
    )

    assert manifest["version"] == 1
    assert manifest["task"]["run_id"] == "run-1"
    assert manifest["task"]["model"] == "resolved-model"
    assert manifest["task"]["provider_id"] == "openrouter"
    assert manifest["tools"]["active_names"] == ["web_fetch"]
    assert manifest["tools"]["unavailable_requested_names"] == ["missing_tool"]
    assert manifest["tools"]["schema_chars_by_tool"]["web_fetch"] > 0
    assert manifest["prompt_sections_chars"]["task"] == len(spec.task)
    assert manifest["policy"]["allowed_path_count"] == 1
    assert manifest["memory"]["selected_ids"] == [
        "memory_fact:fact-1",
        "memory_entry:entry-2",
    ]
    assert "Secret task text" not in str(manifest)
    assert "secret input" not in str(manifest)


def test_worker_recipe_context_is_bounded_advisory_and_manifest_is_content_free() -> None:
    definition = {
        "applicability_conditions": ["The fixture is local."],
        "required_capabilities": ["filesystem_read"],
        "required_permissions": ["filesystem_read"],
        "strategy_steps": ["Inspect the fixture before changing it."],
        "verification_contract": {"required_checks": ["pytest"]},
        "known_failures": [],
        "invalidating_conditions": ["The target is production."],
    }
    recipe = ProceduralRecipeContext(
        id=f"recipe_{'a' * 64}",
        evaluation_id=f"recipe_eval_{'b' * 64}",
        definition_fingerprint=procedural_recipe_definition_fingerprint(definition),
        **definition,
    )
    spec = WorkerSpec(
        id="worker-recipe",
        task="Inspect the fixture",
        inputs={},
        system_prompt="Work carefully.",
        available_tools=["filesystem_read"],
        granted_capabilities=[],
        timeout_seconds=30,
        max_thinking_steps=4,
        effective_permissions=["filesystem_read"],
        procedural_recipes=[recipe],
    )

    prompt = _build_procedural_recipe_prompt(spec)
    manifest = _build_worker_context_manifest(
        spec=spec,
        tools=[],
        prompt_sections={"procedural_memory": prompt},
    )

    assert "Inspect the fixture before changing it." in prompt
    assert "cannot change" in prompt
    assert manifest["memory"]["recipe_ids"] == [recipe.id]
    assert manifest["memory"]["recipe_definition_fingerprints"] == {
        recipe.id: recipe.definition_fingerprint
    }
    assert manifest["memory"]["recipe_evaluation_ids"] == [recipe.evaluation_id]
    assert "Inspect the fixture" not in str(manifest)
