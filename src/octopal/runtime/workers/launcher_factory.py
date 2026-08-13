from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import shutil
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

import structlog

from octopal.infrastructure.config.settings import Settings
from octopal.runtime.workers.launcher import DockerLauncher, SameEnvLauncher, WorkerLauncher

logger = structlog.get_logger(__name__)
_DOCKER_STATUS_CACHE_TTL_SECONDS = 10.0
_WORKER_IMAGE_FINGERPRINT_LABEL = "io.octopal.worker-image-fingerprint"
_WORKER_ENTRYPOINT_MODULE = "octopal.runtime.workers.entrypoint"
_WORKER_IMAGE_BUILD_INPUTS = ("docker/Dockerfile", "pyproject.toml", "uv.lock")
_WORKER_RUNTIME_RESOURCE_INPUTS = ("src/octopal/runtime/octo/prompts/worker_system.md",)
_docker_status_cache: dict[tuple[str, str, str, str, str], tuple[float, WorkerLauncherStatus]] = {}


class _DynamicWorkerImportError(RuntimeError):
    pass


class _RuntimeImportVisitor(ast.NodeVisitor):
    _DYNAMIC_IMPORT_CALLS = {
        "entry_points",
        "find_loader",
        "import_module",
        "iter_modules",
        "load_module",
        "walk_packages",
    }

    def __init__(self, package: str) -> None:
        self.package = package
        self.modules: set[str] = set()
        self.dynamic_import_bindings = {"__import__"}
        self.dynamic_import = False

    def visit_If(self, node: ast.If) -> None:
        if _is_type_checking_guard(node.test):
            for child in node.orelse:
                self.visit(child)
            return
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        self.modules.update(alias.name for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if node.level:
            relative_name = f"{'.' * node.level}{module}"
            module = importlib.util.resolve_name(relative_name, self.package)
        if not module:
            return
        self.modules.add(module)
        self.modules.update(f"{module}.{alias.name}" for alias in node.names if alias.name != "*")
        if module in {"importlib", "importlib.metadata", "pkgutil"}:
            self.dynamic_import_bindings.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name in self._DYNAMIC_IMPORT_CALLS
            )

    def visit_Call(self, node: ast.Call) -> None:
        is_dynamic_import_binding = (
            isinstance(node.func, ast.Name) and node.func.id in self.dynamic_import_bindings
        )
        is_dynamic_import_call = (
            isinstance(node.func, ast.Attribute) and node.func.attr in self._DYNAMIC_IMPORT_CALLS
        )
        if is_dynamic_import_binding or is_dynamic_import_call:
            self.dynamic_import = True
        self.generic_visit(node)


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


def _get_worker_launcher_status(
    settings: Settings, *, auto_build_image: bool
) -> WorkerLauncherStatus:
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
            effective_launcher="unavailable",
            available=False,
            reason=docker_detail,
        )

    docker_cli_path = docker_detail
    daemon_result, daemon_error = _run_docker_command(
        [docker_cli_path, "info", "--format", "{{.ServerVersion}}"],
        timeout=5,
    )
    if daemon_error is not None or daemon_result is None:
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="unavailable",
            available=False,
            reason=f"Docker daemon is unavailable: {daemon_error or 'no result returned'}",
            docker_cli_path=docker_cli_path,
            docker_daemon_reachable=False,
        )
    if daemon_result.returncode != 0:
        detail = (
            daemon_result.stderr or daemon_result.stdout or ""
        ).strip() or "Docker daemon is unavailable."
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="unavailable",
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
    if image_error is not None or image_result is None:
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="unavailable",
            available=False,
            reason=f"Docker image check failed: {image_error or 'no result returned'}",
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

            build_detail = build_error
            if build_detail is None and build_result is not None:
                build_detail = (
                    build_result.stderr or build_result.stdout or ""
                ).strip() or "docker build failed."
            build_detail = build_detail or "docker build failed without details."
            return WorkerLauncherStatus(
                configured_launcher="docker",
                effective_launcher="unavailable",
                available=False,
                reason=(
                    f"Docker image '{image_name}' is not available and automatic build failed: {build_detail} "
                    f"Run 'uv run octopal build-worker-image --tag {image_name}'."
                ),
                docker_cli_path=docker_cli_path,
                docker_daemon_reachable=True,
                docker_image_present=False,
            )
        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="unavailable",
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
            effective_launcher="unavailable",
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

            build_detail = build_error
            if build_detail is None and build_result is not None:
                build_detail = (
                    build_result.stderr or build_result.stdout or ""
                ).strip() or "docker build failed."
            build_detail = build_detail or "docker build failed without details."
            return WorkerLauncherStatus(
                configured_launcher="docker",
                effective_launcher="unavailable",
                available=False,
                reason=(
                    f"Docker image '{image_name}' is stale and automatic rebuild failed: {build_detail} "
                    f"Run 'uv run octopal build-worker-image --tag {image_name}'."
                ),
                docker_cli_path=docker_cli_path,
                docker_daemon_reachable=True,
                docker_image_present=True,
            )

        return WorkerLauncherStatus(
            configured_launcher="docker",
            effective_launcher="unavailable",
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
    inputs = set(_WORKER_IMAGE_BUILD_INPUTS)
    inputs.update(_WORKER_RUNTIME_RESOURCE_INPUTS)
    src_root = project_root / "src"
    if not src_root.exists():
        return sorted(path for path in inputs if (project_root / path).is_file())

    try:
        inputs.update(
            _iter_local_runtime_imports(
                project_root,
                entrypoint_module=_WORKER_ENTRYPOINT_MODULE,
            )
        )
    except (OSError, SyntaxError, ValueError, _DynamicWorkerImportError) as exc:
        logger.warning(
            "Worker runtime import surface could not be resolved; fingerprinting all source files",
            error_type=type(exc).__name__,
        )
        inputs.update(_iter_all_python_source_inputs(project_root))

    return sorted(path for path in inputs if (project_root / path).is_file())


def _iter_local_runtime_imports(project_root: Path, *, entrypoint_module: str) -> list[str]:
    pending = deque([entrypoint_module])
    visited: set[str] = set()
    inputs: set[str] = set()

    while pending:
        module_name = pending.popleft()
        if module_name in visited:
            continue
        visited.add(module_name)

        module_path = _resolve_local_module(project_root, module_name)
        if module_path is None:
            continue
        inputs.add(module_path.relative_to(project_root).as_posix())

        for package_name in _parent_package_names(module_name):
            if package_name not in visited:
                pending.append(package_name)

        source = module_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(module_path))
        package = (
            module_name if module_path.name == "__init__.py" else module_name.rpartition(".")[0]
        )
        visitor = _RuntimeImportVisitor(package)
        visitor.visit(tree)
        if visitor.dynamic_import:
            raise _DynamicWorkerImportError(module_name)

        for imported_module in sorted(visitor.modules):
            if imported_module == "octopal" or imported_module.startswith("octopal."):
                pending.append(imported_module)

    return sorted(inputs)


def _resolve_local_module(project_root: Path, module_name: str) -> Path | None:
    module_path = project_root / "src" / Path(*module_name.split("."))
    source_file = module_path.with_suffix(".py")
    if source_file.is_file():
        return source_file
    package_file = module_path / "__init__.py"
    if package_file.is_file():
        return package_file
    return None


def _parent_package_names(module_name: str) -> list[str]:
    parts = module_name.split(".")
    return [".".join(parts[:idx]) for idx in range(1, len(parts))]


def _is_type_checking_guard(node: ast.expr) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "TYPE_CHECKING"
    return isinstance(node, ast.Attribute) and node.attr == "TYPE_CHECKING"


def _iter_all_python_source_inputs(project_root: Path) -> list[str]:
    src_root = project_root / "src"
    return [
        path.relative_to(project_root).as_posix()
        for path in sorted(src_root.rglob("*.py"))
        if path.is_file() and "__pycache__" not in path.parts
    ]


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
        raise RuntimeError(
            "Docker worker isolation is configured but unavailable: "
            f"{launcher_status.reason} "
            "Restore Docker or explicitly set workers.launcher to 'same_env' for trusted local development."
        )
    return SameEnvLauncher()
