from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Protocol


class WorkerLauncher(Protocol):
    async def launch(
        self,
        spec_path: str,
        cwd: str,
        env: dict[str, str],
    ) -> asyncio.subprocess.Process: ...


@dataclass
class SameEnvLauncher:
    entrypoint_module: str = "octopal.runtime.workers.entrypoint"

    async def launch(
        self,
        spec_path: str,
        cwd: str,
        env: dict[str, str],
    ) -> asyncio.subprocess.Process:
        popen_kwargs = _worker_subprocess_kwargs()
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            self.entrypoint_module,
            spec_path,
            cwd=cwd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **popen_kwargs,
        )


@dataclass
class DockerLauncher:
    image: str
    host_workspace: str
    container_workspace: str = "/workspace"
    entrypoint_module: str = "octopal.runtime.workers.entrypoint"

    async def launch(
        self,
        spec_path: str,
        cwd: str,
        env: dict[str, str],
    ) -> asyncio.subprocess.Process:
        import json
        from pathlib import Path

        worker_id = os.path.basename(cwd.rstrip(os.sep))
        container_ws = self.container_workspace
        if not container_ws.startswith("/"):
            container_ws = "/" + container_ws

        # Look for allowed_paths in the spec
        allowed_paths = None
        try:
            with open(spec_path, encoding="utf-8") as f:
                spec_data = json.load(f)
                allowed_paths = spec_data.get("allowed_paths")
        except Exception:
            pass

        cmd_args = ["docker", "run", "--rm", "-i"]
        user_spec = _host_user_spec()
        if user_spec:
            cmd_args.extend(["--user", user_spec])

        host_worker_dir = Path(cwd).resolve()
        container_worker_dir = f"{container_ws}/workers/{worker_id}"
        cmd_args.extend(["-v", f"{host_worker_dir}:{container_worker_dir}"])
        container_env = _filter_container_env(env, worker_workspace=container_worker_dir)
        for key, value in container_env.items():
            cmd_args.extend(["-e", f"{key}={value}"])

        host_ws_path = Path(self.host_workspace).resolve()
        seen_mounts: set[tuple[str, str]] = set()
        host_skills_dir = host_ws_path / "skills"
        host_skills_dir.mkdir(parents=True, exist_ok=True)
        cmd_args.extend(["-v", f"{host_skills_dir}:{container_worker_dir}/skills"])
        seen_mounts.add((str(host_skills_dir), f"{container_worker_dir}/skills"))
        for rel_path in allowed_paths or []:
            if not isinstance(rel_path, str) or not rel_path.strip():
                continue
            host_path = (host_ws_path / rel_path).resolve()
            try:
                host_path.relative_to(host_ws_path)
            except ValueError:
                continue
            if not host_path.exists():
                continue
            mount_targets = (
                f"{container_ws}/{rel_path}",
                f"{container_worker_dir}/{rel_path}",
            )
            for mount_target in mount_targets:
                mount_key = (str(host_path), mount_target)
                if mount_key in seen_mounts:
                    continue
                cmd_args.extend(["-v", f"{host_path}:{mount_target}"])
                seen_mounts.add(mount_key)

        spec_in_container = f"{container_ws}/workers/{worker_id}/spec.json"
        cmd_args.extend([
            "-w",
            f"{container_ws}/workers/{worker_id}",
            "-e",
            f"OCTOPAL_WORKER_SPEC={spec_in_container}",
            self.image,
            "python",
            "-m",
            self.entrypoint_module,
            spec_in_container,
        ])

        popen_kwargs = _worker_subprocess_kwargs()
        return await asyncio.create_subprocess_exec(
            *cmd_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
            **popen_kwargs,
        )


def _filter_container_env(
    env: dict[str, str], *, worker_workspace: str | None = None
) -> dict[str, str]:
    # Container env must be explicit; keep only a safe subset.
    allowed = {
        "PYTHONPATH",
        "OCTOPAL_WORKSPACE_DIR",
        "LITELLM_NUM_RETRIES",
        "LITELLM_TIMEOUT",
        "LITELLM_FALLBACKS",
        "LITELLM_DROP_PARAMS",
        "LITELLM_CACHING",
        "LITELLM_MAX_CONCURRENCY",
        "LITELLM_RATE_LIMIT_MAX_RETRIES",
        "LITELLM_RATE_LIMIT_BASE_DELAY_SECONDS",
        "LITELLM_RATE_LIMIT_MAX_DELAY_SECONDS",
        "BRAVE_API_KEY",
        "FIRECRAWL_API_KEY",
    }
    filtered = {key: value for key, value in env.items() if key in allowed}
    if worker_workspace:
        filtered["OCTOPAL_WORKSPACE_DIR"] = worker_workspace
    return filtered


def _host_user_spec() -> str | None:
    if os.name == "nt":
        return None
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if not callable(getuid) or not callable(getgid):
        return None
    uid = getuid()
    gid = getgid()
    if uid < 0 or gid < 0:
        return None
    return f"{uid}:{gid}"


def _worker_subprocess_kwargs() -> dict[str, object]:
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}
