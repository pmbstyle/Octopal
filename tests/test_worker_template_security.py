from __future__ import annotations

from pathlib import Path

from broodmind.tools.workers.management import _tool_create_worker_template


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
