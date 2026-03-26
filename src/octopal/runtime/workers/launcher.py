from __future__ import annotations

import asyncio
import os
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

        if allowed_paths is not None:
            host_worker_dir = Path(cwd).resolve()
            container_worker_dir = f"{container_ws}/workers/{worker_id}"
            cmd_args.extend(["-v", f"{host_worker_dir}:{container_worker_dir}"])

            host_ws_path = Path(self.host_workspace).resolve()
            seen_mounts: set[tuple[str, str]] = set()
            for rel_path in allowed_paths:
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
        else:
            # Legacy fallback: mount the whole workspace
            cmd_args.extend(["-v", f"{self.host_workspace}:{self.container_workspace}"])

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

        return await asyncio.create_subprocess_exec(
            *cmd_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_filter_env(env),
        )


def _filter_env(env: dict[str, str]) -> dict[str, str]:
    # Docker env must be explicit; keep only a safe subset.
    allowed = {"PYTHONPATH", "OCTOPAL_WORKSPACE_DIR"}
    return {key: value for key, value in env.items() if key in allowed}
