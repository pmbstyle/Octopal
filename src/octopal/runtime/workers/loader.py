"""
Filesystem-based Worker Template Loader

Auto-discovers worker templates from the configured workspace's workers directory.
Each worker is defined in a worker.json file with JSON configuration.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import structlog

from octopal.infrastructure.store.models import WorkerTemplateRecord

logger = structlog.get_logger(__name__)
_WORKER_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def discover_worker_templates(workspace_dir: Path) -> list[WorkerTemplateRecord]:
    """
    Discover all worker templates from the configured workspace directory.

    Each worker directory should contain a worker.json file.
    """
    workers_dir = workspace_dir / "workers"
    if not workers_dir.exists():
        logger.warning(f"Workers directory not found: {workers_dir}")
        return []

    templates = []
    for worker_path in workers_dir.iterdir():
        if not worker_path.is_dir():
            continue

        worker_file = worker_path / "worker.json"
        if not worker_file.exists():
            logger.debug(f"No worker.json found in {worker_path}")
            continue

        try:
            template = _load_worker_template(worker_file)
            if template:
                templates.append(template)
        except Exception as e:
            logger.error(f"Failed to load worker from {worker_file}: {e}")

    return templates


def get_worker_template(workspace_dir: Path, worker_id: str) -> WorkerTemplateRecord | None:
    """
    Load a specific worker template by ID.

    Returns None if the worker doesn't exist.
    """
    if not _WORKER_ID_PATTERN.fullmatch(worker_id):
        logger.warning("Invalid worker template id: %s", worker_id)
        return None
    workers_root = (workspace_dir / "workers").resolve()
    worker_file = (workers_root / worker_id / "worker.json").resolve()
    try:
        worker_file.relative_to(workers_root)
    except ValueError:
        logger.warning("Worker template path escapes workers root: %s", worker_id)
        return None
    if not worker_file.exists():
        return None

    try:
        return _load_worker_template(worker_file)
    except Exception as e:
        logger.error(f"Failed to load worker {worker_id}: {e}")
        return None


def _load_worker_template(worker_file: Path) -> WorkerTemplateRecord | None:
    """
    Load a worker template from a worker.json file.

    The file must be valid JSON with all required fields.
    """
    try:
        config_text = worker_file.read_text(encoding="utf-8")
        config = json.loads(config_text)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {worker_file}: {e}")
        return None
    except Exception as e:
        logger.error(f"Cannot read {worker_file}: {e}")
        return None

    # Validate required fields
    required_fields = [
        "id",
        "name",
        "description",
        "system_prompt",
        "available_tools",
        "required_permissions",
        "max_thinking_steps",
        "default_timeout_seconds",
    ]

    missing = [f for f in required_fields if f not in config]
    if missing:
        logger.error(f"Missing required fields in {worker_file}: {missing}")
        return None

    # Validate types
    if not isinstance(config["available_tools"], list):
        logger.error(f"available_tools must be a list in {worker_file}")
        return None

    if not isinstance(config["required_permissions"], list):
        logger.error(f"required_permissions must be a list in {worker_file}")
        return None

    if not isinstance(config["max_thinking_steps"], int):
        logger.error(f"max_thinking_steps must be an int in {worker_file}")
        return None

    if not isinstance(config["default_timeout_seconds"], int):
        logger.error(f"default_timeout_seconds must be an int in {worker_file}")
        return None

    can_spawn_children = bool(config.get("can_spawn_children", False))
    allowed_child_templates = config.get("allowed_child_templates", [])
    if not isinstance(allowed_child_templates, list) or any(
        not isinstance(item, str) for item in allowed_child_templates
    ):
        logger.error(f"allowed_child_templates must be a list of strings in {worker_file}")
        return None
    allowed_paths = config.get("allowed_paths", [])
    if not isinstance(allowed_paths, list) or any(
        not isinstance(item, str) for item in allowed_paths
    ):
        logger.error(f"allowed_paths must be a list of strings in {worker_file}")
        return None

    # Get file modification time for updated_at
    mtime = worker_file.stat().st_mtime
    updated_at = datetime.fromtimestamp(mtime, tz=UTC)

    # Create WorkerTemplateRecord
    return WorkerTemplateRecord(
        id=config["id"],
        name=config["name"],
        description=config["description"],
        system_prompt=config["system_prompt"],
        available_tools=config["available_tools"],
        required_permissions=config["required_permissions"],
        model=config.get("model"),
        max_thinking_steps=config["max_thinking_steps"],
        default_timeout_seconds=config["default_timeout_seconds"],
        can_spawn_children=can_spawn_children,
        allowed_child_templates=allowed_child_templates,
        allowed_paths=[item.strip() for item in allowed_paths if item.strip()],
        created_at=updated_at,  # Use file mtime for both
        updated_at=updated_at,
    )
