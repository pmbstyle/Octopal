from __future__ import annotations

import hashlib
import io
import json
import os
import platform
import secrets
import shutil
import socket
import subprocess
import tarfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import structlog

from octopal.infrastructure.config.settings import Settings

logger = structlog.get_logger(__name__)

_MANAGED_LABEL = "io.octopal.managed-web"
_PINCHTAB_CONTAINER_PORT = 9867
_ACTIVE_CONTAINER: str | None = None
_ACTIVE_SETTINGS: Settings | None = None
_WEBCLAW_VERSION = "0.6.14"
_WEBCLAW_ASSETS = {
    ("Darwin", "arm64"): (
        "webclaw-v0.6.14-aarch64-apple-darwin.tar.gz",
        "d4af184422e411f1762ae38d94d57439607bbf61da3991c797e13502c66ac4fe",
    ),
    ("Darwin", "x86_64"): (
        "webclaw-v0.6.14-x86_64-apple-darwin.tar.gz",
        "0299163d10830e32950dcb9c4473a9c61a6215ccfa27732d49d06abe44bbf639",
    ),
    ("Linux", "aarch64"): (
        "webclaw-v0.6.14-aarch64-unknown-linux-gnu.tar.gz",
        "54ff83beb29d0c257f6c322e279cae0fd0ea151ff6c7499a454c9dfdc495d0f0",
    ),
    ("Linux", "x86_64"): (
        "webclaw-v0.6.14-x86_64-unknown-linux-gnu.tar.gz",
        "a16bb7d7335d938c7c9ccfdcfb392601fcc7b42e4e2e4f7757c4a150882584f5",
    ),
}


@dataclass(frozen=True)
class ManagedPinchTabStatus:
    status: str
    detail: str
    base_url: str | None = None
    worker_base_url: str | None = None
    container_name: str | None = None
    image: str | None = None


def prepare_managed_web_runtime(settings: Settings) -> ManagedPinchTabStatus:
    """Resolve the default browser stack without making startup depend on PinchTab."""
    _prepare_host_webclaw(settings)
    backend = str(settings.browser_backend or "auto").strip().lower() or "auto"
    if backend == "playwright":
        status = ManagedPinchTabStatus("disabled", "Playwright was selected explicitly.")
        _write_metadata(settings, status)
        return status

    if settings.pinchtab_token or settings.pinchtab_session:
        settings.browser_backend = "pinchtab"
        status = ManagedPinchTabStatus(
            "external",
            "Using the configured PinchTab service.",
            base_url=settings.pinchtab_base_url,
            worker_base_url=settings.pinchtab_worker_base_url,
        )
        _write_metadata(settings, status)
        return status

    if not settings.pinchtab_managed:
        if backend == "auto":
            settings.browser_backend = "playwright"
        status = ManagedPinchTabStatus(
            "disabled",
            "Managed PinchTab is disabled; using Playwright.",
        )
        _write_metadata(settings, status)
        return status

    try:
        status, token = _ensure_managed_pinchtab(settings)
    except Exception as exc:
        settings.browser_backend = "playwright"
        detail = f"Managed PinchTab unavailable; using Playwright: {exc}"
        logger.warning("Managed PinchTab startup degraded", error=str(exc))
        _write_metadata(settings, ManagedPinchTabStatus("degraded", detail))
        return ManagedPinchTabStatus("degraded", detail)

    global _ACTIVE_SETTINGS
    settings.browser_backend = "pinchtab"
    settings.pinchtab_base_url = str(status.base_url)
    settings.pinchtab_worker_base_url = status.worker_base_url
    settings.pinchtab_token = token
    _ACTIVE_SETTINGS = settings
    _write_metadata(settings, status)
    logger.info(
        "Managed PinchTab ready",
        base_url=status.base_url,
        worker_base_url=status.worker_base_url,
        container=status.container_name,
        image=status.image,
    )
    return status


def stop_managed_web_runtime() -> None:
    global _ACTIVE_CONTAINER, _ACTIVE_SETTINGS
    from octopal.browser.pinchtab import configure_pinchtab_backend

    configure_pinchtab_backend(None)
    container = _ACTIVE_CONTAINER
    settings = _ACTIVE_SETTINGS
    _ACTIVE_CONTAINER = None
    _ACTIVE_SETTINGS = None
    if not container:
        return
    result = _docker(["rm", "-f", container], timeout=30)
    if result.returncode == 0:
        logger.info("Managed PinchTab stopped", container=container)
        if settings is not None:
            _write_metadata(
                settings,
                ManagedPinchTabStatus(
                    "stopped",
                    "Managed PinchTab is stopped with Octopal.",
                    container_name=container,
                    image=settings.pinchtab_image,
                ),
            )
    else:
        logger.warning(
            "Failed to stop managed PinchTab",
            container=container,
            error=_command_detail(result),
        )


def read_managed_web_status(settings: Settings) -> ManagedPinchTabStatus:
    path = _managed_root(settings) / "status.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return ManagedPinchTabStatus("unknown", "Managed web runtime has not started yet.")
    return ManagedPinchTabStatus(
        status=str(payload.get("status") or "unknown"),
        detail=str(payload.get("detail") or ""),
        base_url=_optional_text(payload.get("base_url")),
        worker_base_url=_optional_text(payload.get("worker_base_url")),
        container_name=_optional_text(payload.get("container_name")),
        image=_optional_text(payload.get("image")),
    )


def _ensure_managed_pinchtab(
    settings: Settings,
) -> tuple[ManagedPinchTabStatus, str]:
    global _ACTIVE_CONTAINER
    if not _docker_available():
        raise RuntimeError("Docker is unavailable")

    image = str(settings.pinchtab_image or "pinchtab/pinchtab:0.11.0").strip()
    if not image:
        raise RuntimeError("PinchTab image is empty")
    _ensure_image(image)

    root = _managed_root(settings)
    data_dir = root / "data"
    config_path = root / "config.json"
    root.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    token = _load_or_create_token(config_path)
    _write_pinchtab_config(config_path, token)

    instance_key = hashlib.sha256(str(Path(settings.state_dir).resolve()).encode()).hexdigest()[:10]
    container = f"octopal-pinchtab-{instance_key}"
    existing_port = _running_container_port(container, image=image)
    if existing_port is not None:
        base_url = f"http://127.0.0.1:{existing_port}"
        if _wait_for_health(base_url, token, timeout_seconds=3.0):
            _ACTIVE_CONTAINER = container
            return _ready_status(settings, container, image, existing_port), token

    _remove_container(container)
    port = _reserve_local_port()
    result = _docker(
        [
            "run",
            "-d",
            "--name",
            container,
            "--restart",
            "unless-stopped",
            "--label",
            f"{_MANAGED_LABEL}={instance_key}",
            "--shm-size=2g",
            "-p",
            f"127.0.0.1:{port}:{_PINCHTAB_CONTAINER_PORT}",
            "-e",
            "PINCHTAB_CONFIG=/config/config.json",
            "-v",
            f"{config_path.resolve()}:/config/config.json:ro",
            "-v",
            f"{data_dir.resolve()}:/data",
            image,
        ],
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"failed to start container: {_command_detail(result)}")

    base_url = f"http://127.0.0.1:{port}"
    if not _wait_for_health(base_url, token, timeout_seconds=45.0):
        logs = _docker(["logs", "--tail", "40", container], timeout=15)
        _remove_container(container)
        raise RuntimeError(f"health check failed: {_command_detail(logs)}")

    _ACTIVE_CONTAINER = container
    return _ready_status(settings, container, image, port), token


def _ready_status(
    settings: Settings, container: str, image: str, port: int
) -> ManagedPinchTabStatus:
    base_url = f"http://127.0.0.1:{port}"
    worker_url = (
        f"http://host.docker.internal:{port}"
        if str(settings.worker_launcher or "").strip().lower() == "docker"
        else base_url
    )
    return ManagedPinchTabStatus(
        "ready",
        "Managed PinchTab is healthy.",
        base_url=base_url,
        worker_base_url=worker_url,
        container_name=container,
        image=image,
    )


def _managed_root(settings: Settings) -> Path:
    return Path(settings.state_dir) / "managed-web" / "pinchtab"


def _prepare_host_webclaw(settings: Settings) -> None:
    if not settings.webclaw_enabled:
        return
    configured = str(settings.webclaw_binary or "webclaw").strip() or "webclaw"
    resolved = shutil.which(configured)
    if resolved:
        settings.webclaw_binary = resolved
        return
    if configured != "webclaw":
        logger.warning("Configured WebClaw binary is unavailable", binary=configured)
        return

    asset_info = _WEBCLAW_ASSETS.get((platform.system(), platform.machine().lower()))
    if asset_info is None:
        logger.warning(
            "Managed WebClaw is unavailable on this platform",
            system=platform.system(),
            machine=platform.machine(),
        )
        return

    target_dir = Path(settings.state_dir) / "managed-web" / "webclaw" / _WEBCLAW_VERSION
    target = target_dir / ("webclaw.exe" if os.name == "nt" else "webclaw")
    if target.is_file() and os.access(target, os.X_OK):
        settings.webclaw_binary = str(target)
        return

    asset, expected_sha = asset_info
    url = f"https://github.com/0xMassi/webclaw/releases/download/v{_WEBCLAW_VERSION}/{asset}"
    try:
        response = httpx.get(url, follow_redirects=True, timeout=180.0)
        response.raise_for_status()
        archive = response.content
        actual_sha = hashlib.sha256(archive).hexdigest()
        if actual_sha != expected_sha:
            raise RuntimeError(
                f"checksum mismatch for {asset}: expected {expected_sha}, got {actual_sha}"
            )
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            member = next(
                (item for item in tar.getmembers() if Path(item.name).name == "webclaw"),
                None,
            )
            if member is None or not member.isfile():
                raise RuntimeError("release archive does not contain the WebClaw binary")
            source = tar.extractfile(member)
            if source is None:
                raise RuntimeError("failed to read WebClaw from release archive")
            target_dir.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix(".tmp")
            temporary.write_bytes(source.read())
            temporary.chmod(0o755)
            temporary.replace(target)
    except Exception as exc:
        logger.warning("Managed WebClaw install degraded", error=str(exc))
        return

    settings.webclaw_binary = str(target)
    logger.info("Managed WebClaw ready", binary=str(target), version=_WEBCLAW_VERSION)


def _write_pinchtab_config(path: Path, token: str) -> None:
    payload = {
        "server": {"port": str(_PINCHTAB_CONTAINER_PORT), "bind": "0.0.0.0", "token": token},
        "instanceDefaults": {
            "mode": "headless",
            "stealthLevel": "medium",
            "maxTabs": 20,
            "tabEvictionPolicy": "close_lru",
        },
        "security": {
            "allowedDomains": ["*"],
            "allowEvaluate": False,
            "allowCookies": False,
            "allowDownload": False,
            "allowUpload": False,
            "allowClipboard": False,
            "enableActionGuards": True,
            "idpi": {
                "enabled": True,
                "strictMode": True,
                "scanContent": True,
                "wrapContent": True,
                "shieldThreshold": 0,
            },
        },
        "observability": {"activity": {"enabled": True, "retentionDays": 7}},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    path.chmod(0o600)


def _load_or_create_token(config_path: Path) -> str:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        token = str(payload.get("server", {}).get("token") or "").strip()
        if token:
            return token
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return secrets.token_urlsafe(32)


def _write_metadata(settings: Settings, status: ManagedPinchTabStatus) -> None:
    root = _managed_root(settings)
    root.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "status": status.status,
        "detail": status.detail,
        "base_url": status.base_url,
        "worker_base_url": status.worker_base_url,
        "container_name": status.container_name,
        "image": status.image,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    (root / "status.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _docker_available() -> bool:
    try:
        result = _docker(["info", "--format", "{{.ServerVersion}}"], timeout=8)
    except OSError:
        return False
    return result.returncode == 0


def _ensure_image(image: str) -> None:
    inspect = _docker(["image", "inspect", image], timeout=15)
    if inspect.returncode == 0:
        return
    pull = _docker(["pull", image], timeout=600)
    if pull.returncode != 0:
        raise RuntimeError(f"failed to pull {image}: {_command_detail(pull)}")


def _running_container_port(container: str, *, image: str) -> int | None:
    inspect = _docker(
        ["inspect", "-f", "{{.State.Running}}|{{.Config.Image}}", container], timeout=10
    )
    if inspect.returncode != 0:
        return None
    running, _, actual_image = inspect.stdout.strip().partition("|")
    if running != "true" or actual_image != image:
        return None
    port_result = _docker(["port", container, f"{_PINCHTAB_CONTAINER_PORT}/tcp"], timeout=10)
    if port_result.returncode != 0:
        return None
    try:
        return int(port_result.stdout.strip().rsplit(":", 1)[1])
    except (IndexError, ValueError):
        return None


def _remove_container(container: str) -> None:
    _docker(["rm", "-f", container], timeout=30)


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_health(base_url: str, token: str, *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    headers = {"Authorization": f"Bearer {token}"}
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{base_url}/health", headers=headers, timeout=2.0)
            if response.status_code == 200:
                payload = response.json()
                if isinstance(payload, dict) and payload.get("status") == "ok":
                    return True
        except (httpx.HTTPError, ValueError):
            pass
        time.sleep(0.25)
    return False


def _docker(args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _command_detail(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr or result.stdout or "command failed").strip()[-2000:]


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
