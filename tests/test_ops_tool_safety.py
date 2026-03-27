from __future__ import annotations

import json
from pathlib import Path

import octopal.tools.ops.exec_run as exec_run_tools
import octopal.tools.ops.management as ops_tools
from octopal.tools.ops.management import docker_compose_control, test_run


def test_test_run_rejects_shell_chaining(tmp_path: Path) -> None:
    result = test_run({"command": "pytest && whoami"}, {"base_dir": tmp_path})
    assert result.startswith("test_run error:")


def test_docker_exec_preserves_quoted_args(monkeypatch, tmp_path: Path) -> None:
    captured: list[str] = []

    def fake_run_command(command: list[str], cwd: Path | None = None, timeout_seconds: int = 30):
        captured[:] = command
        return 0, "ok", ""

    monkeypatch.setattr(ops_tools, "_run_command", fake_run_command)
    monkeypatch.setenv("OCTOPAL_ALLOWED_SERVICES", "api")

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


def test_cleanup_background_sessions_terminates_and_clears_registry(monkeypatch) -> None:
    class _Pipe:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class _Proc:
        def __init__(self) -> None:
            self.pid = 1234
            self.stdin = _Pipe()
            self.stdout = _Pipe()
            self.stderr = _Pipe()

        def poll(self):
            return None

    proc = _Proc()
    original_registry = dict(exec_run_tools._PROCESS_REGISTRY)
    exec_run_tools._PROCESS_REGISTRY.clear()
    exec_run_tools._PROCESS_REGISTRY["session-1"] = {"process": proc, "buffer": object()}
    terminated: list[int] = []

    def _fake_terminate(session: dict) -> None:
        terminated.append(session["process"].pid)

    monkeypatch.setattr(exec_run_tools, "_terminate_session_process", _fake_terminate)

    try:
        cleaned = exec_run_tools.cleanup_background_sessions()
    finally:
        exec_run_tools._PROCESS_REGISTRY.clear()
        exec_run_tools._PROCESS_REGISTRY.update(original_registry)

    assert cleaned == 1
    assert terminated == [1234]
    assert proc.stdin.closed is True
    assert proc.stdout.closed is True
    assert proc.stderr.closed is True
    assert exec_run_tools._PROCESS_REGISTRY == original_registry
