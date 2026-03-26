from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from octopal.runtime.workers.launcher import DockerLauncher


def test_docker_launcher_mounts_only_worker_dir_when_allowed_paths_missing(
    tmp_path: Path, monkeypatch
) -> None:
    workspace = tmp_path / "workspace"
    worker_dir = workspace / "workers" / "worker-1"
    worker_dir.mkdir(parents=True, exist_ok=True)
    spec_path = worker_dir / "spec.json"
    spec_path.write_text(json.dumps({"id": "worker-1"}), encoding="utf-8")

    captured: dict[str, object] = {}

    async def _fake_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace(pid=123)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    launcher = DockerLauncher(image="octopal:test", host_workspace=str(workspace))
    asyncio.run(
        launcher.launch(
            spec_path=str(spec_path),
            cwd=str(worker_dir),
            env={"PYTHONPATH": "src", "OCTOPAL_WORKSPACE_DIR": "/workspace", "SECRET": "nope"},
        )
    )

    args = captured["args"]
    assert f"{worker_dir}:/workspace/workers/worker-1" in args
    assert f"{workspace}:/workspace" not in args
    assert "SECRET" not in captured["kwargs"]["env"]
    assert captured["kwargs"]["env"]["OCTOPAL_WORKSPACE_DIR"] == "/workspace"


def test_docker_launcher_mounts_worker_dir_and_shared_paths_when_restricted(
    tmp_path: Path, monkeypatch
) -> None:
    workspace = tmp_path / "workspace"
    worker_dir = workspace / "workers" / "worker-1"
    shared_dir = workspace / "src"
    worker_dir.mkdir(parents=True, exist_ok=True)
    shared_dir.mkdir(parents=True, exist_ok=True)
    spec_path = worker_dir / "spec.json"
    spec_path.write_text(json.dumps({"id": "worker-1", "allowed_paths": ["src"]}), encoding="utf-8")

    captured: dict[str, object] = {}

    async def _fake_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace(pid=123)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    launcher = DockerLauncher(image="octopal:test", host_workspace=str(workspace))
    asyncio.run(
        launcher.launch(
            spec_path=str(spec_path),
            cwd=str(worker_dir),
            env={"PYTHONPATH": "src", "OCTOPAL_WORKSPACE_DIR": "/workspace"},
        )
    )

    args = captured["args"]
    assert f"{worker_dir}:/workspace/workers/worker-1" in args
    assert f"{shared_dir}:/workspace/src" in args
    assert f"{shared_dir}:/workspace/workers/worker-1/src" in args
    assert f"{workspace}:/workspace" not in args
