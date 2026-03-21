from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from broodmind.cli.main import app

runner = CliRunner()


def test_skill_install_command_installs_local_bundle(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = tmp_path / "writer"
    source_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )

    result = runner.invoke(app, ["skill", "install", str(source_dir), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "installed"
    assert payload["skill_id"] == "writer"


def test_skill_list_command_reads_installed_manifest(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    (workspace_dir / "skills").mkdir(parents=True)
    (workspace_dir / "skills" / "installed.json").write_text(
        json.dumps(
            {
                "version": 1,
                "installs": [
                    {
                        "skill_id": "writer",
                        "source": "zanblayde/agent-commons",
                        "source_kind": "clawhub_slug",
                        "path": str(workspace_dir / "skills" / "writer" / "SKILL.md"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )

    result = runner.invoke(app, ["skill", "list", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["installs"][0]["skill_id"] == "writer"


def test_skill_update_command_uses_saved_source(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = tmp_path / "writer"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "SKILL.md"
    source_file.write_text(
        """---
name: writer
description: v1
---
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )

    install_result = runner.invoke(app, ["skill", "install", str(source_dir), "--json"])
    assert install_result.exit_code == 0
    source_file.write_text(
        """---
name: writer
description: v2
---
""",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["skill", "update", "writer", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "updated"


def test_skill_remove_command_deletes_installed_skill(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = tmp_path / "writer"
    source_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )

    install_result = runner.invoke(app, ["skill", "install", str(source_dir), "--json"])
    assert install_result.exit_code == 0

    result = runner.invoke(app, ["skill", "remove", "writer", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "removed"
    assert not (workspace_dir / "skills" / "writer").exists()


def test_skill_trust_and_untrust_commands_toggle_manifest_state(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = tmp_path / "writer"
    source_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )

    install_result = runner.invoke(app, ["skill", "install", str(source_dir), "--json"])
    assert install_result.exit_code == 0

    untrust_result = runner.invoke(app, ["skill", "untrust", "writer", "--json"])
    assert untrust_result.exit_code == 0
    untrust_payload = json.loads(untrust_result.stdout)
    assert untrust_payload["status"] == "untrusted"
    assert untrust_payload["trusted"] is False

    trust_result = runner.invoke(app, ["skill", "trust", "writer", "--json"])
    assert trust_result.exit_code == 0
    trust_payload = json.loads(trust_result.stdout)
    assert trust_payload["status"] == "trusted"
    assert trust_payload["trusted"] is True
