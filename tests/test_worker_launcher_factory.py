from __future__ import annotations

import subprocess
from pathlib import Path

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


def test_worker_runtime_config_defaults_to_docker() -> None:
    config = WorkerRuntimeConfig()
    assert config.launcher == "docker"


def test_settings_default_worker_launcher_is_docker() -> None:
    settings = Settings()
    assert settings.worker_launcher == "docker"


def test_detect_docker_cli_reports_missing_when_not_on_path(monkeypatch) -> None:
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: None)
    ok, detail = detect_docker_cli()
    assert ok is False
    assert "not found" in detail.lower()


def _mock_docker_ready(monkeypatch, *, image_present: bool = True) -> None:
    launcher_factory._docker_status_cache.clear()
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker")
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
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker")
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
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker")
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


def test_build_launcher_returns_docker_launcher_when_cli_is_available(monkeypatch, tmp_path: Path) -> None:
    _mock_docker_ready(monkeypatch)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )

    launcher = build_launcher(settings)
    assert isinstance(launcher, DockerLauncher)


def test_build_launcher_falls_back_to_same_env_when_docker_cli_is_missing(monkeypatch, tmp_path: Path) -> None:
    launcher_factory._docker_status_cache.clear()
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: None)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )

    launcher = build_launcher(settings)
    assert isinstance(launcher, SameEnvLauncher)


def test_build_launcher_falls_back_when_docker_daemon_is_unavailable(monkeypatch, tmp_path: Path) -> None:
    launcher_factory._docker_status_cache.clear()
    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.shutil.which", lambda name: "/usr/bin/docker")

    def _fake_run(cmd, capture_output=True, text=True, timeout=5):
        if cmd[1:3] == ["info", "--format"]:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="Cannot connect to the Docker daemon")
        raise AssertionError(f"Unexpected docker command: {cmd}")

    monkeypatch.setattr("octopal.runtime.workers.launcher_factory.subprocess.run", _fake_run)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
    )

    launcher = build_launcher(settings)
    status = get_worker_launcher_status(settings)
    assert isinstance(launcher, SameEnvLauncher)
    assert status.effective_launcher == "same_env"
    assert "daemon" in status.reason.lower()


def test_status_reports_missing_worker_image_without_auto_build(monkeypatch, tmp_path: Path) -> None:
    _mock_docker_ready(monkeypatch, image_present=False)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    status = get_worker_launcher_status(settings)
    assert status.effective_launcher == "same_env"
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
    assert any("io.octopal.worker-image-fingerprint=fingerprint-1" in part for cmd in commands for part in cmd)


def test_passive_status_does_not_block_later_auto_build(monkeypatch, tmp_path: Path) -> None:
    commands = _mock_docker_with_autobuild(monkeypatch, build_succeeds=True)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    passive_status = get_worker_launcher_status(settings)
    ensured_status = ensure_worker_launcher_status(settings)

    assert passive_status.effective_launcher == "same_env"
    assert "build-worker-image" in passive_status.reason
    assert ensured_status.effective_launcher == "docker"
    assert "built automatically" in ensured_status.reason
    assert any(cmd[1] == "build" for cmd in commands)


def test_build_launcher_falls_back_when_auto_build_fails(monkeypatch, tmp_path: Path) -> None:
    commands = _mock_docker_with_autobuild(monkeypatch, build_succeeds=False)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    launcher = build_launcher(settings)
    status = ensure_worker_launcher_status(settings)
    assert isinstance(launcher, SameEnvLauncher)
    assert status.effective_launcher == "same_env"
    assert "automatic build failed" in status.reason
    assert "build-worker-image" in status.reason
    assert any(cmd[1] == "build" for cmd in commands)


def test_status_reports_stale_worker_image_without_auto_rebuild(monkeypatch, tmp_path: Path) -> None:
    _mock_docker_with_stale_image(monkeypatch, rebuild_succeeds=True)
    settings = Settings(
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
        OCTOPAL_WORKER_LAUNCHER="docker",
        OCTOPAL_WORKER_DOCKER_IMAGE="octopal-worker:latest",
    )

    status = get_worker_launcher_status(settings)
    assert status.effective_launcher == "same_env"
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
    assert any("io.octopal.worker-image-fingerprint=fingerprint-new" in part for cmd in commands for part in cmd)
