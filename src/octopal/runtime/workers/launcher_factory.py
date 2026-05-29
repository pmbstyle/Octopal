from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from octopal.infrastructure.config.settings import Settings
from octopal.runtime.workers.launcher import DockerLauncher, SameEnvLauncher, WorkerLauncher

logger = structlog.get_logger(__name__)
_DOCKER_STATUS_CACHE_TTL_SECONDS = 10.0
_WORKER_IMAGE_FINGERPRINT_LABEL = "io.octopal.worker-image-fingerprint"
_docker_status_cache: dict[tuple[str, str, str], tuple[float, WorkerLauncherStatus]] = {}


@dataclass(frozen=True)
class WorkerLauncherStatus:
    configured_launcher: str
    effective_launcher: str
    available: bool
    reason: str
    docker_cli_path: str | None = None
    docker_daemon_reachable: bool | None = None
    docker_image_present: bool | None = None


def detect_docker_cli() -> tuple[bool, str]:
    docker_path = shutil.which("docker")
    if not docker_path:
        return False, "Docker CLI was not found on PATH."
    return True, docker_path


def get_worker_launcher_status(settings: Settings) -> WorkerLauncherStatus:
    return _get_worker_launcher_status(settings, auto_build_image=False)


def ensure_worker_launcher_status(settings: Settings) -> WorkerLauncherStatus:
    return _get_worker_launcher_status(settings, auto_build_image=True)


def _get_worker_launcher_status(settings: Settings, *, auto_build_image: bool) -> WorkerLauncherStatus:
    configured = str(settings.worker_launcher or "same_env").strip() or "same_env"
    project_root = Path(__file__).resolve().parents[4]
    image_fingerprint = _compute_worker_image_fingerprint(project_root)
    cache_key = (
        configured,
        str(settings.worker_docker_image or "").strip(),
        str(settings.worker_docker_host_workspace or settings.workspace_dir),
        image_fingerprint,
        "ensure" if auto_build_image else "status",
    )
    cached = _docker_status_cache.get(cache_key)
    now = time.monotonic()
    if cached and now - cached[0] < _DOCKER_STATUS_CACHE_TTL_SECONDS:
        return cached[1]

    status = _compute_worker_launcher_status(
        settings,
        auto_build_image=auto_build_image,
        image_fingerprint=image_fingerprint,
        project_root=project_root,
    )
    _docker_status_cache[cache_key] = (now, status)
    return status


def _compute_worker_launcher_status(
    settings: Settings,
    *,
    auto_build_image: bool,
    image_fingerprint: str,
    project_root: Path,
) -> WorkerLauncherStatus:
    configured = str(settings.worker_launcher or "same_env").strip() or "same_env"
    if configured != "docker":
        return WorkerLauncherStatus(
            configured_launcher=configured,
            effective_launcher="same_env",
            available=True,
            reason="Docker launcher is not configured.",
        )

    docker_ok, docker_detail = detect_docker_cli()
    if not docker_ok:
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=docker_detail,
        )

    docker_cli_path = docker_detail
    daemon_result, daemon_error = _run_docker_command(
        [docker_cli_path, "info", "--format", "{{.ServerVersion}}"],
        timeout=5,
    )
    if daemon_error is not None:
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=f"Docker daemon is unavailable: {daemon_error}",
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=False,
        )
    if daemon_result.returncode != 0:
        detail = (daemon_result.stderr or daemon_result.stdout or "").strip() or "Docker daemon is unavailable."
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=f"Docker daemon is unavailable: {detail}",
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=False,
        )

    image_name = str(settings.worker_docker_image or "").strip() or "octopal-worker:latest"
    image_result, image_error = _run_docker_command(
        [docker_cli_path, "image", "inspect", image_name],
        timeout=5,
    )
    if image_error is not None:
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=f"Docker image check failed: {image_error}",
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=True,
            docker_image_present=False,
        )
    if image_result.returncode != 0:
        if auto_build_image:
            logger.info("Docker worker image missing; attempting automatic build", image=image_name)
            build_result, build_error = _build_worker_image(
                docker_cli_path,
                image_name,
                image_fingerprint=image_fingerprint,
                project_root=project_root,
            )
            if build_error is None and build_result is not None and build_result.returncode == 0:
                logger.info("Docker worker image built successfully", image=image_name)
                return WorkerLauncherStatus(
                    configured_launcher="docker",
                    effective_launcher="docker",
                    available=True,
                    reason=f"Docker worker image '{image_name}' was built automatically and is ready.",
                    docker_cli_path=docker_cli_path,
                    docker_daemon_reachable=True,
                    docker_image_present=True,
                )

            detail = build_error
            if detail is None and build_result is not None:
                detail = (build_result.stderr or build_result.stdout or "").strip() or "docker build failed."
            return WorkerLauncherStatus(
                configured_launcher="docker",
                effective_launcher="same_env",
                available=False,
                reason=(
                    f"Docker image '{image_name}' is not available and automatic build failed: {detail} "
                    f"Run 'uv run octopal build-worker-image --tag {image_name}'."
                ),
                docker_cli_path=docker_cli_path,
                docker_daemon_reachable=True,
                docker_image_present=False,
            )
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=(
                f"Docker image '{image_name}' is not available. "
                f"Build it with 'uv run octopal build-worker-image --tag {image_name}'."
            ),
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=True,
            docker_image_present=False,
        )

    image_label, label_error = _read_image_label(
        docker_cli_path,
        image_name,
        _WORKER_IMAGE_FINGERPRINT_LABEL,
    )
    if label_error is not None:
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=f"Docker image metadata check failed: {label_error}",
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=True,
            docker_image_present=True,
        )

    if image_label != image_fingerprint:
        if auto_build_image:
            logger.info(
                "Docker worker image is stale; attempting automatic rebuild",
                image=image_name,
            )
            build_result, build_error = _build_worker_image(
                docker_cli_path,
                image_name,
                image_fingerprint=image_fingerprint,
                project_root=project_root,
            )
            if build_error is None and build_result is not None and build_result.returncode == 0:
                logger.info("Docker worker image rebuilt successfully", image=image_name)
                return WorkerLauncherStatus(
                    configured_launcher="docker",
                    effective_launcher="docker",
                    available=True,
                    reason=(
                        f"Docker worker image '{image_name}' was rebuilt automatically because "
                        "worker build inputs changed."
                    ),
                    docker_cli_path=docker_cli_path,
                    docker_daemon_reachable=True,
                    docker_image_present=True,
                )

            detail = build_error
            if detail is None and build_result is not None:
                detail = (build_result.stderr or build_result.stdout or "").strip() or "docker build failed."
            return WorkerLauncherStatus(
                configured_launcher="docker",
                effective_launcher="same_env",
                available=False,
                reason=(
                    f"Docker image '{image_name}' is stale and automatic rebuild failed: {detail} "
                    f"Run 'uv run octopal build-worker-image --tag {image_name}'."
                ),
                docker_cli_path=docker_cli_path,
                docker_daemon_reachable=True,
                docker_image_present=True,
            )

        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="same_env",
            available=False,
            reason=(
                f"Docker image '{image_name}' is stale because worker build inputs changed. "
                f"Build it with 'uv run octopal build-worker-image --tag {image_name}'."
            ),
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=True,
            docker_image_present=True,
        )

    return WorkerLauncherStatus(
        configured_launcher="docker",
        effective_launcher="docker",
        available=True,
        reason="Docker worker runtime is ready.",
        docker_cli_path=docker_cli_path,
        docker_daemon_reachable=True,
        docker_image_present=True,
    )


def _run_docker_command(
    cmd: list[str], *, timeout: int
) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    try:
        return (
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            ),
            None,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return None, str(exc)


def _read_image_label(
    docker_cli_path: str,
    image_name: str,
    label_name: str,
) -> tuple[str | None, str | None]:
    result, error = _run_docker_command(
        [
            docker_cli_path,
            "image",
            "inspect",
            image_name,
            "--format",
            "{{ json .Config.Labels }}",
        ],
        timeout=5,
    )
    if error is not None:
        return None, error
    if result is None or result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip() if result is not None else ""
        return None, detail or "docker image inspect failed."
    raw = (result.stdout or "").strip() or "null"
    try:
        labels = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid docker image labels payload: {exc}"
    if not isinstance(labels, dict):
        return None, None
    value = labels.get(label_name)
    return str(value).strip() if value is not None else None, None


def _compute_worker_image_fingerprint(project_root: Path) -> str:
    digest = hashlib.sha256()
    for rel_path in _iter_worker_image_inputs(project_root):
        full_path = project_root / rel_path
        digest.update(rel_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(full_path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _iter_worker_image_inputs(project_root: Path) -> list[str]:
    inputs = ["docker/Dockerfile", "pyproject.toml", "README.md"]
    src_root = project_root / "src"
    if src_root.exists():
        for path in sorted(src_root.rglob("*")):
            if not path.is_file():
                continue
            if "__pycache__" in path.parts:
                continue
            inputs.append(path.relative_to(project_root).as_posix())
    return inputs


def _build_worker_image(
    docker_cli_path: str,
    image_name: str,
    *,
    image_fingerprint: str,
    project_root: Path,
) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    dockerfile = project_root / "docker" / "Dockerfile"
    if not dockerfile.exists():
        return None, f"Dockerfile not found: {dockerfile}"
    return _run_docker_command(
        [
            docker_cli_path,
            "build",
            "--target",
            "worker",
            "-t",
            image_name,
            "--label",
            f"{_WORKER_IMAGE_FINGERPRINT_LABEL}={image_fingerprint}",
            "-f",
            str(dockerfile),
            str(project_root),
        ],
        timeout=600,
    )


def build_launcher(settings: Settings) -> WorkerLauncher:
    launcher_status = ensure_worker_launcher_status(settings)
    if launcher_status.effective_launcher == "docker":
        host_workspace = settings.worker_docker_host_workspace
        if not host_workspace:
            host_workspace = str(settings.workspace_dir.resolve())
        return DockerLauncher(
            image=settings.worker_docker_image,
            host_workspace=host_workspace,
            container_workspace=settings.worker_docker_workspace,
        )

    if settings.worker_launcher == "docker":
        logger.warning(
            "Docker launcher requested but unavailable; falling back to same_env",
            reason=launcher_status.reason,
        )
    return SameEnvLauncher()
