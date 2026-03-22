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


def test_skill_list_command_includes_local_skill(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
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

    result = runner.invoke(app, ["skill", "list", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["skills"][0]["id"] == "writer"
    assert payload["skills"][0]["installer_managed"] is False


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


def test_skill_remove_command_deletes_local_skill(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
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

    result = runner.invoke(app, ["skill", "remove", "writer", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "removed"
    assert not (workspace_dir / "skills" / "writer").exists()


def test_skill_trust_and_untrust_commands_toggle_local_skill_state(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = workspace_dir / "skills" / "writer"
    scripts_dir = source_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "noop.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )

    untrust_result = runner.invoke(app, ["skill", "untrust", "writer", "--json"])
    assert untrust_result.exit_code == 0
    untrust_payload = json.loads(untrust_result.stdout)
    assert untrust_payload["status"] == "untrusted"
    assert untrust_payload["trusted"] is False

    listed = runner.invoke(app, ["skill", "list", "--json"])
    listed_payload = json.loads(listed.stdout)
    assert listed_payload["skills"][0]["trusted"] is False

    trust_result = runner.invoke(app, ["skill", "trust", "writer", "--json"])
    assert trust_result.exit_code == 0
    trust_payload = json.loads(trust_result.stdout)
    assert trust_payload["status"] == "trusted"
    assert trust_payload["trusted"] is True


def test_skill_trust_requires_force_when_scan_has_findings(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---
""",
        encoding="utf-8",
    )
    (workspace_dir / "skills" / "installed.json").write_text(
        json.dumps(
            {
                "version": 1,
                "installs": [
                    {
                        "skill_id": "writer",
                        "source": "zanblayde/agent-commons",
                        "source_kind": "clawhub_slug",
                        "trusted": False,
                        "has_scripts": True,
                        "path": str(skill_dir / "SKILL.md"),
                        "script_scan": {
                            "status": "review_required",
                            "scanned_at": "2026-03-21T10:00:00+00:00",
                            "file_count": 1,
                            "files": [],
                            "findings": [{"path": "scripts/fetch.py", "rule": "network_access"}],
                        },
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

    blocked = runner.invoke(app, ["skill", "trust", "writer", "--json"])
    assert blocked.exit_code == 1
    blocked_payload = json.loads(blocked.stdout)
    assert blocked_payload["status"] == "error"

    allowed = runner.invoke(app, ["skill", "trust", "writer", "--force", "--json"])
    assert allowed.exit_code == 0
    allowed_payload = json.loads(allowed.stdout)
    assert allowed_payload["status"] == "trusted"


def test_skill_install_command_auto_prepares_env_and_returns_trust_next_step(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "job-search"
    source_dir = tmp_path / "job-search"
    scripts_dir = source_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: job-search
description: Search jobs
metadata:
  {
    "broodmind": {
      "runtime": {
        "python": {
          "packages": ["python-jobspy"]
        }
      }
    }
  }
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "jobspy.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(
        "broodmind.cli.main.load_settings",
        lambda: SimpleNamespace(workspace_dir=workspace_dir),
    )
    monkeypatch.setattr(
        "broodmind.tools.skills.installer.prepare_skill_env",
        lambda skill_id, workspace_dir: {
            "status": "prepared",
            "skill_id": skill_id,
            "kind": "python",
        },
    )

    result = runner.invoke(app, ["skill", "install", str(source_dir), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["env_prepared"] is True
    assert payload["env_kind"] == "python"
