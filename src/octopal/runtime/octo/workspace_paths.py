from __future__ import annotations

import os
from pathlib import Path


def _workspace_dir() -> Path:
    return Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()
