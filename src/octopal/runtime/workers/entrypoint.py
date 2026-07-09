"""
Simplified Worker Entrypoint

All workers use the same agent_worker with different system prompts.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from octopal.runtime.workers.agent_worker import run_agent_worker


def main() -> None:
    spec_path = None
    if len(sys.argv) >= 2:
        spec_path = sys.argv[1]
    if not spec_path:
        spec_path = os.getenv("OCTOPAL_WORKER_SPEC")
    if not spec_path:
        raise SystemExit("spec path required")

    spec_path = _resolve_spec_path(spec_path)
    asyncio.run(run_agent_worker(spec_path))


def _resolve_spec_path(spec_path: str) -> str:
    candidate_paths = [spec_path]
    if not spec_path.startswith("/"):
        candidate_paths.append(f"/{spec_path}")
    for candidate in candidate_paths:
        if Path(candidate).exists():
            return candidate
    return spec_path


if __name__ == "__main__":
    main()
