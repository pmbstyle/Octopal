from __future__ import annotations

import asyncio
import json
from pathlib import Path

from broodmind.queen.core import Queen
from broodmind.tools.ops_tools import docker_compose_control, test_run
from broodmind.tools.worker_tools import _tool_create_worker_template
from broodmind.workers.contracts import WorkerResult


def test_test_run_rejects_shell_chaining(tmp_path: Path) -> None:
    result = test_run({"command": "pytest && whoami"}, {"base_dir": tmp_path})
    assert result.startswith("test_run error:")


def test_create_worker_template_rejects_path_traversal(tmp_path: Path) -> None:
    class DummyStore:
        def get_worker_template(self, template_id: str):
            return None

    class DummyQueen:
        def __init__(self) -> None:
            self.store = DummyStore()

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    result = _tool_create_worker_template(
        {
            "id": "../escape",
            "name": "Bad",
            "description": "Bad",
            "system_prompt": "Bad",
        },
        {"queen": DummyQueen(), "base_dir": workspace},
    )
    assert "error" in result.lower()


def test_docker_exec_preserves_quoted_args(monkeypatch, tmp_path: Path) -> None:
    captured: list[str] = []

    def fake_run_command(command: list[str], cwd: Path | None = None, timeout_seconds: int = 30):
        captured[:] = command
        return 0, "ok", ""

    import broodmind.tools.ops_tools as ops_tools

    monkeypatch.setattr(ops_tools, "_run_command", fake_run_command)
    monkeypatch.setenv("BROODMIND_ALLOWED_SERVICES", "api")

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")

    result = docker_compose_control(
        {
            "action": "exec",
            "services": ["api"],
            "command": 'echo "hello world"',
            "compose_file": "docker-compose.yml",
            "confirm": True,
        },
        {"base_dir": workspace},
    )

    payload = json.loads(result)
    assert payload["returncode"] == 0
    assert "api" in captured
    api_index = captured.index("api")
    assert captured[api_index + 1 :] == ["echo", "hello world"]


def test_queen_passes_approval_requester_to_runtime(monkeypatch) -> None:
    class DummyRuntime:
        def __init__(self) -> None:
            self.captured = None

        async def run_task(self, task_request, approval_requester=None):
            self.captured = approval_requester
            return WorkerResult(summary="ok")

    class DummyApprovals:
        bot = None

    class DummyMemory:
        async def add_message(self, role: str, text: str, metadata: dict):
            return None

    async def fake_bootstrap_context(store, chat_id: int):
        from broodmind.queen.prompt_builder import BootstrapContext

        return BootstrapContext(content="", hash="", files=[])

    async def fake_route_or_reply(
        queen,
        provider,
        memory,
        user_text: str,
        chat_id: int,
        bootstrap_context: str,
        show_typing: bool = True,
    ):
        return "ok"

    import broodmind.queen.core as queen_core

    monkeypatch.setattr(queen_core, "build_bootstrap_context_prompt", fake_bootstrap_context)
    monkeypatch.setattr(queen_core, "route_or_reply", fake_route_or_reply)

    runtime = DummyRuntime()
    queen = Queen(
        provider=object(),
        store=object(),
        policy=object(),
        runtime=runtime,
        approvals=DummyApprovals(),
        memory=DummyMemory(),
        canon=object(),
    )

    async def requester(intent) -> bool:
        return True

    async def scenario() -> None:
        await queen.handle_message("hello", 123, approval_requester=requester)
        await queen._start_worker_async(
            worker_id="coder",
            task="do thing",
            chat_id=123,
            inputs={},
            tools=None,
            model=None,
            timeout_seconds=5,
        )
        await asyncio.sleep(0.05)
        assert runtime.captured is requester

    asyncio.run(scenario())
