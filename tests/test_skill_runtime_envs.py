from __future__ import annotations

import json
from pathlib import Path

from broodmind.tools.skills.runtime_envs import (
    get_skill_env_status,
    prepare_skill_env,
    remove_skill_env,
)


def test_get_skill_env_status_reports_required_python_env(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "job-search"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
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

    status = get_skill_env_status("job-search", workspace_dir=workspace_dir)

    assert status["kind"] == "python"
    assert status["required"] is True
    assert status["prepared"] is False
    assert "prepare-env job-search" in status["next_step"]


def test_prepare_skill_env_creates_python_env_manifest(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "job-search"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
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

    def _fake_run(command, check, capture_output, text, cwd=None):
        if command[:3] == [__import__("sys").executable, "-m", "venv"]:
            env_dir = Path(command[3])
            scripts_path = env_dir / ("Scripts" if __import__("os").name == "nt" else "bin")
            scripts_path.mkdir(parents=True, exist_ok=True)
            python_name = "python.exe" if __import__("os").name == "nt" else "python"
            (scripts_path / python_name).write_text("", encoding="utf-8")
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr("broodmind.tools.skills.runtime_envs.subprocess.run", _fake_run)

    payload = prepare_skill_env("job-search", workspace_dir=workspace_dir)

    assert payload["status"] == "prepared"
    manifest = json.loads((workspace_dir / ".skill-envs" / "job-search" / "env.json").read_text(encoding="utf-8"))
    assert manifest["kind"] == "python"
    assert manifest["python_packages"] == ["python-jobspy"]


def test_remove_skill_env_deletes_env_dir(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    env_dir = workspace_dir / ".skill-envs" / "job-search"
    env_dir.mkdir(parents=True)
    (env_dir / "env.json").write_text("{}", encoding="utf-8")

    payload = remove_skill_env("job-search", workspace_dir=workspace_dir)

    assert payload["status"] == "removed"
    assert not env_dir.exists()


def test_prepare_skill_env_creates_node_env_manifest(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "ui-helper"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: ui-helper
description: Run TS helpers
metadata:
  {
    "broodmind": {
      "runtime": {
        "node": {
          "packages": ["tsx"],
          "packageManager": "npm"
        }
      }
    }
  }
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "helper.ts").write_text("console.log('ok')\n", encoding="utf-8")

    def _fake_run(command, cwd=None, check=True, capture_output=True, text=True):
        if command[:3] == ["npm", "install", "--no-save"]:
            node_bin = Path(cwd) / "node_modules" / ".bin"
            node_bin.mkdir(parents=True, exist_ok=True)
            binary_name = "tsx.cmd" if __import__("os").name == "nt" else "tsx"
            (node_bin / binary_name).write_text("", encoding="utf-8")
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr("broodmind.tools.skills.runtime_envs.subprocess.run", _fake_run)
    monkeypatch.setattr("broodmind.tools.skills.runtime_envs.shutil.which", lambda name: "npm" if name in {"npm", "node"} else None)

    payload = prepare_skill_env("ui-helper", workspace_dir=workspace_dir)

    assert payload["status"] == "prepared"
    assert payload["kind"] == "node"
    manifest = json.loads((workspace_dir / ".skill-envs" / "ui-helper" / "env.json").read_text(encoding="utf-8"))
    assert manifest["node_packages"] == ["tsx"]
