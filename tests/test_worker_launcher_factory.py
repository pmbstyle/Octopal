from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import octopal.runtime.workers.launcher_factory as launcher_factory
from octopal.infrastructure.config.models import WorkerRuntimeConfig
from octopal.infrastructure.config.settings import Settings
from octopal.runtime.workers.launcher import DockerLauncher, SameEnvLauncher
from octopal.runtime.workers.launcher_factory import (
    build_launcher,
    detect_docker_cli,
    ensure_worker_launcher_status,
    get_worker_launcher_status,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_project_file(project_root: Path, relative_path: str, content: str) -> None:
    path = project_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _minimal_worker_project(project_root: Path) -> None:
    files = {
        "docker/Dockerfile": "FROM python:3.12-slim\n",
        "pyproject.toml": "[project]\nname = 'fixture'\n",
        "uv.lock": "fixture-lock\n",
        "README.md": "fixture docs\n",
        "src/octopal/__init__.py": "",
        "src/octopal/runtime/__init__.py": "",
        "src/octopal/runtime/workers/__init__.py": "",
        "src/octopal/runtime/workers/entrypoint.py": (
            "from octopal.runtime.workers.agent_worker import run_agent_worker\n"
        ),
        "src/octopal/runtime/workers/agent_worker.py": (
            "from typing import TYPE_CHECKING\n"
            "from octopal.runtime.worker_support import worker_value\n"
            "if TYPE_CHECKING:\n"
            "    from octopal.runtime.octo.router import route_or_reply\n"
            "def run_agent_worker():\n"
            "    return worker_value\n"
        ),
        "src/octopal/runtime/worker_support.py": "worker_value = 1\n",
        "src/octopal/runtime/octo/__init__.py": "",
        "src/octopal/runtime/octo/router.py": "route_or_reply = None\n",
        "src/octopal/runtime/memory/service.py": "memory_service = None\n",
        "src/octopal/channels/telegram/handlers.py": "telegram_handler = None\n",
        "src/octopal/runtime/octo/prompts/octo_system.md": "main prompt\n",
        "src/octopal/runtime/octo/prompts/worker_system.md": "worker prompt\n",
    }
    for relative_path, content in files.items():
        _write_project_file(project_root, relative_path, content)


def test_worker_runtime_config_defaults_to_docker() -> None:
    config = WorkerRuntimeConfig()
    assert config.launcher == "docker"


def test_settings_default_worker_launcher_is_docker() -> None:
    settings = Settings()
    assert settings.worker_launcher == "docker"


def test_worker_image_fingerprint_includes_dependency_lock(tmp_path: Path) -> None:
    _minimal_worker_project(tmp_path)

    first = launcher_factory._compute_worker_image_fingerprint(tmp_path)
    (tmp_path / "uv.lock").write_text("changed", encoding="utf-8")
    second = launcher_factory._compute_worker_image_fingerprint(tmp_path)

    assert first != second


def test_worker_image_inputs_follow_actual_runtime_import_surface() -> None:
    inputs = set(launcher_factory._iter_worker_image_inputs(_PROJECT_ROOT))

    assert {
        "docker/Dockerfile",
        "pyproject.toml",
        "uv.lock",
        "src/octopal/runtime/workers/entrypoint.py",
        "src/octopal/runtime/workers/agent_worker.py",
        "src/octopal/runtime/octo/prompts/worker_system.md",
        "src/octopal/infrastructure/providers/factory.py",
        "src/octopal/tools/filesystem/files.py",
    } <= inputs
    assert "README.md" not in inputs
    assert "src/octopal/runtime/octo/router.py" not in inputs
    assert "src/octopal/runtime/memory/service.py" not in inputs
    assert "src/octopal/channels/telegram/handlers.py" not in inputs
    assert "src/octopal/runtime/octo/prompts/octo_system.md" not in inputs


@pytest.mark.parametrize(
    "relative_path",
    [
        "README.md",
        "src/octopal/runtime/octo/router.py",
        "src/octopal/runtime/memory/service.py",
        "src/octopal/channels/telegram/handlers.py",
        "src/octopal/runtime/octo/prompts/octo_system.md",
    ],
)
def test_irrelevant_main_process_changes_do_not_change_worker_fingerprint(
    tmp_path: Path,
    relative_path: str,
) -> None:
    _minimal_worker_project(tmp_path)
    first = launcher_factory._compute_worker_image_fingerprint(tmp_path)

    _write_project_file(tmp_path, relative_path, "changed but not imported by worker\n")

    assert launcher_factory._compute_worker_image_fingerprint(tmp_path) == first


@pytest.mark.parametrize(
    "relative_path",
    [
        "docker/Dockerfile",
        "pyproject.toml",
        "uv.lock",
        "src/octopal/runtime/workers/entrypoint.py",
        "src/octopal/runtime/worker_support.py",
        "src/octopal/runtime/octo/prompts/worker_system.md",
    ],
)
def test_worker_runtime_and_build_changes_change_worker_fingerprint(
    tmp_path: Path,
    relative_path: str,
) -> None:
    _minimal_worker_project(tmp_path)
    first = launcher_factory._compute_worker_image_fingerprint(tmp_path)

    path = tmp_path / relative_path
    path.write_text(path.read_text(encoding="utf-8") + "# changed\n", encoding="utf-8")

    assert launcher_factory._compute_worker_image_fingerprint(tmp_path) != first


@pytest.mark.parametrize(
    "dynamic_import",
    [
        'import importlib\nimportlib.import_module("octopal.runtime.optional_worker")\n',
        (
            "from importlib import import_module\n"
            'import_module("octopal.runtime.optional_worker")\n'
        ),
        (
            "from importlib import import_module as load_optional\n"
            'load_optional("octopal.runtime.optional_worker")\n'
        ),
        "from importlib.metadata import entry_points as plugins\nplugins()\n",
    ],
)
def test_dynamic_worker_imports_fail_closed_to_all_python_source(
    tmp_path: Path,
    dynamic_import: str,
) -> None:
    _minimal_worker_project(tmp_path)
    agent_worker = tmp_path / "src/octopal/runtime/workers/agent_worker.py"
    agent_worker.write_text(
        agent_worker.read_text(encoding="utf-8") + dynamic_import,
        encoding="utf-8",
    )
    first = launcher_factory._compute_worker_image_fingerprint(tmp_path)

    unrelated = tmp_path / "src/octopal/runtime/octo/router.py"
    unrelated.write_text("changed after dynamic import\n", encoding="utf-8")

    assert launcher_factory._compute_worker_image_fingerprint(tmp_path) != first


def test_detect_docker_cli_reports_missing_when_not_on_path(monkeypatch) -> None:
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: None)
    ok, detail = detect_docker_cli()
    assert ok is False
    assert "not found" in detail.lower()


def _mock_docker_ready(monkeypatch, *, image_present: bool = True) -> None:
    launcher_factory._docker_status_cache.clear()
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker"
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory._compute_worker_image_fingerprint",
        lambda project_root: "fingerprint-1",
    )

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[1:3] == ["info", "--format"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="27.0.1\n", stderr="")
        if cmd[1:3] == ["image", "inspect"]:
            if len(cmd) >= 6 and cmd[4] == "--format":
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout='{"io.octopal.worker-image-fingerprint":"fingerprint-1"}',
                    stderr="",
                )
            if image_present:
                return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="No such image")
        raise AssertionError(f"Unexpected docker command: {cmd}")

    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.subprocess.run", _fake_run)


def _mock_docker_with_autobuild(monkeypatch, *, build_succeeds: bool) -> list[list[str]]:
    launcher_factory._docker_status_cache.clear()
    commands: list[list[str]] = []
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker"
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory._compute_worker_image_fingerprint",
        lambda project_root: "fingerprint-1",
    )

    image_present = False

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        nonlocal image_present
        commands.append(cmd)
        if cmd[1:3] == ["info", "--format"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="27.0.1\n", stderr="")
        if cmd[1:3] == ["image", "inspect"]:
            if len(cmd) >= 6 and cmd[4] == "--format":
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout='{"io.octopal.worker-image-fingerprint":"fingerprint-1"}',
                    stderr="",
                )
            if image_present:
                return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="No such image")
        if cmd[1] == "build":
            if build_succeeds:
                image_present = True
                return subprocess.CompletedProcess(cmd, 0, stdout="built", stderr="")
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="Build failed")
        raise AssertionError(f"Unexpected docker command: {cmd}")

    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.subprocess.run", _fake_run)
    return commands


def _mock_docker_with_stale_image(monkeypatch, *, rebuild_succeeds: bool) -> list[list[str]]:
    launcher_factory._docker_status_cache.clear()
    commands: list[list[str]] = []
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker"
    )
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory._compute_worker_image_fingerprint",
        lambda project_root: "fingerprint-new",
    )

    image_label = "fingerprint-old"

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        nonlocal image_label
        commands.append(cmd)
        if cmd[1:3] == ["info", "--format"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="27.0.1\n", stderr="")
        if cmd[1:3] == ["image", "inspect"]:
            if len(cmd) >= 6 and cmd[4] == "--format":
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout=f'{{"io.octopal.worker-image-fingerprint":"{image_label}"}}',
                    stderr="",
                )
            return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")
        if cmd[1] == "build":
            if rebuild_succeeds:
                image_label = "fingerprint-new"
                return subprocess.CompletedProcess(cmd, 0, stdout="rebuilt", stderr="")
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="Build failed")
        raise AssertionError(f"Unexpected docker command: {cmd}")

    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.subprocess.run", _fake_run)
    return commands


def test_build_launcher_returns_docker_launcher_when_cli_is_available(
    monkeypatch, tmp_path: Path
) -> None:
    _mock_docker_ready(monkeypatch)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )

    launcher = build_launcher(settings)
    assert isinstance(launcher, DockerLauncher)


def test_build_launcher_fails_closed_when_docker_cli_is_missing(
    monkeypatch, tmp_path: Path
) -> None:
    launcher_factory._docker_status_cache.clear()
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: None)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )

    with pytest.raises(RuntimeError, match="Docker worker isolation is configured but unavailable"):
        build_launcher(settings)


def test_build_launcher_fails_closed_when_docker_daemon_is_unavailable(
    monkeypatch, tmp_path: Path
) -> None:
    launcher_factory._docker_status_cache.clear()
    monkeypatch.setattr(
        "octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker"
    )

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[1:3] == ["info", "--format"]:
            return subprocess.CompletedProcess(
                cmd, 1, stdout="", stderr="Cannot connect to the Docker daemon"
            )
        raise AssertionError(f"Unexpected docker command: {cmd}")

    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.subprocess.run", _fake_run)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )

    with pytest.raises(RuntimeError, match="Docker worker isolation is configured but unavailable"):
        build_launcher(settings)
    status = get_worker_launcher_status(settings)
    assert status.effective_launcher == "unavailable"
    assert "daemon" in status.reason.lower()


def test_status_reports_missing_worker_image_without_auto_build(
    monkeypatch, tmp_path: Path
) -> None:
    _mock_docker_ready(monkeypatch, image_present=False)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    status = get_worker_launcher_status(settings)
    assert status.effective_launcher == "unavailable"
    assert "build-worker-image" in status.reason


def test_build_launcher_auto_builds_missing_worker_image(monkeypatch, tmp_path: Path) -> None:
    commands = _mock_docker_with_autobuild(monkeypatch, build_succeeds=True)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    launcher = build_launcher(settings)
    status = ensure_worker_launcher_status(settings)
    assert isinstance(launcher, DockerLauncher)
    assert status.effective_launcher == "docker"
    assert "built automatically" in status.reason
    assert any(cmd[1] == "build" for cmd in commands)
    assert any(
        "io.octopal.worker-image-fingerprint=fingerprint-1" in part
        for cmd in commands
        for part in cmd
    )


def test_passive_status_does_not_block_later_auto_build(monkeypatch, tmp_path: Path) -> None:
    commands = _mock_docker_with_autobuild(monkeypatch, build_succeeds=True)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    passive_status = get_worker_launcher_status(settings)
    ensured_status = ensure_worker_launcher_status(settings)

    assert passive_status.effective_launcher == "unavailable"
    assert "build-worker-image" in passive_status.reason
    assert ensured_status.effective_launcher == "docker"
    assert "built automatically" in ensured_status.reason
    assert any(cmd[1] == "build" for cmd in commands)


def test_build_launcher_fails_closed_when_auto_build_fails(monkeypatch, tmp_path: Path) -> None:
    commands = _mock_docker_with_autobuild(monkeypatch, build_succeeds=False)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    with pytest.raises(RuntimeError, match="Docker worker isolation is configured but unavailable"):
        build_launcher(settings)
    status = ensure_worker_launcher_status(settings)
    assert status.effective_launcher == "unavailable"
    assert "automatic build failed" in status.reason
    assert "build-worker-image" in status.reason
    assert any(cmd[1] == "build" for cmd in commands)


def test_status_reports_stale_worker_image_without_auto_rebuild(
    monkeypatch, tmp_path: Path
) -> None:
    _mock_docker_with_stale_image(monkeypatch, rebuild_succeeds=True)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    status = get_worker_launcher_status(settings)
    assert status.effective_launcher == "unavailable"
    assert "stale" in status.reason
    assert "build-worker-image" in status.reason


def test_build_launcher_auto_rebuilds_stale_worker_image(monkeypatch, tmp_path: Path) -> None:
    commands = _mock_docker_with_stale_image(monkeypatch, rebuild_succeeds=True)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    launcher = build_launcher(settings)
    status = ensure_worker_launcher_status(settings)
    assert isinstance(launcher, DockerLauncher)
    assert status.effective_launcher == "docker"
    assert "rebuilt automatically" in status.reason
    assert any(cmd[1] == "build" for cmd in commands)
    assert any(
        "io.octopal.worker-image-fingerprint=fingerprint-new" in part
        for cmd in commands
        for part in cmd
    )


def test_build_launcher_allows_explicit_same_env(tmp_path: Path) -> None:
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="same_env",
    )

    launcher = build_launcher(settings)

    assert isinstance(launcher, SameEnvLauncher)
