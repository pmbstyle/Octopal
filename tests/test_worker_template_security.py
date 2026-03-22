from __future__ import annotations

import json
from pathlib import Path

from broodmind.tools.workers.management import _tool_create_worker_template, _tool_update_worker_template


def test_create_worker_template_rejects_path_traversal(tmp_path: Path) -> None:
    class DummyStore:
        def get_worker_template(self, template_id: str):
            return None

    class DummyQueen:
        def __init__(self) -> None:
            self.store = DummyStore()

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    result = _tool_create_worker_template(
        {
            "id": "../escape",
            "name": "Bad",
            "description": "Bad",
            "system_prompt": "Bad",
        },
        {"queen": DummyQueen(), "base_dir": workspace},
    )
    assert "error" in result.lower()


def test_create_worker_template_infers_permissions_from_available_tools(tmp_path: Path) -> None:
    class DummyStore:
        def get_worker_template(self, template_id: str):
            return None

    class DummyQueen:
        def __init__(self) -> None:
            self.store = DummyStore()

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    raw = _tool_create_worker_template(
        {
            "id": "python_job_searcher",
            "name": "Python Job Searcher",
            "description": "Search jobs",
            "system_prompt": "Search jobs",
            "available_tools": ["list_skills", "use_skill", "run_skill_script", "fs_read", "fs_write"],
            "required_permissions": ["network"],
        },
        {"queen": DummyQueen(), "base_dir": workspace},
    )
    payload = json.loads(raw)

    assert payload["status"] == "created"
    assert set(payload["required_permissions"]) == {
        "network",
        "skill_manage",
        "skill_use",
        "skill_exec",
        "filesystem_read",
        "filesystem_write",
    }


def test_update_worker_template_infers_permissions_from_available_tools(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    worker_dir = workspace / "workers" / "python_job_searcher"
    worker_dir.mkdir(parents=True, exist_ok=True)
    (worker_dir / "worker.json").write_text(
        json.dumps(
            {
                "id": "python_job_searcher",
                "name": "Python Job Searcher",
                "description": "Search jobs",
                "system_prompt": "Search jobs",
                "available_tools": ["fs_read"],
                "required_permissions": ["network"],
                "model": None,
                "max_thinking_steps": 10,
                "default_timeout_seconds": 300,
                "can_spawn_children": False,
                "allowed_child_templates": [],
            }
        ),
        encoding="utf-8",
    )

    raw = _tool_update_worker_template(
        {
            "id": "python_job_searcher",
            "available_tools": ["list_skills", "use_skill", "run_skill_script"],
        },
        {"base_dir": workspace},
    )
    payload = json.loads(raw)

    assert payload["status"] == "updated"
    stored = json.loads((worker_dir / "worker.json").read_text(encoding="utf-8"))
    assert set(stored["required_permissions"]) == {
        "network",
        "skill_manage",
        "skill_use",
        "skill_exec",
    }
