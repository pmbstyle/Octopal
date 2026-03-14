from __future__ import annotations

import json
from pathlib import Path

import broodmind.tools.ops.management as ops_tools
from broodmind.tools.ops.management import docker_compose_control, test_run


def test_test_run_rejects_shell_chaining(tmp_path: Path) -> None:
    result = test_run({"command": "pytest && whoami"}, {"base_dir": tmp_path})
    assert result.startswith("test_run error:")


def test_docker_exec_preserves_quoted_args(monkeypatch, tmp_path: Path) -> None:
    captured: list[str] = []

    def fake_run_command(command: list[str], cwd: Path | None = None, timeout_seconds: int = 30):
        captured[:] = command
        return 0, "ok", ""

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
