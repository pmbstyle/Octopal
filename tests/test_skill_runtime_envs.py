from __future__ import annotations

import json
import subprocess
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


def test_get_skill_env_status_reads_python_requirements_txt_without_metadata(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "reporter"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: reporter
description: Build reports
---
""",
        encoding="utf-8",
    )
    (skill_dir / "requirements.txt").write_text(
        """
# comment
requests==2.32.0
rich>=13
""".strip(),
        encoding="utf-8",
    )
    (scripts_dir / "report.py").write_text("print('ok')\n", encoding="utf-8")

    status = get_skill_env_status("reporter", workspace_dir=workspace_dir)

    assert status["kind"] == "python"
    assert status["required"] is True
    assert status["python_packages"] == ["requests==2.32.0", "rich>=13"]


def test_get_skill_env_status_reads_package_json_without_metadata(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "ui-helper"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: ui-helper
description: Run TS helpers
---
""",
        encoding="utf-8",
    )
    (skill_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "ui-helper",
                "packageManager": "npm@10.8.0",
                "dependencies": {"chalk": "^5.4.0"},
                "devDependencies": {"tsx": "^4.19.0"},
            }
        ),
        encoding="utf-8",
    )
    (scripts_dir / "helper.ts").write_text("console.log('ok')\n", encoding="utf-8")

    status = get_skill_env_status("ui-helper", workspace_dir=workspace_dir)

    assert status["kind"] == "node"
    assert status["required"] is True
    assert status["package_manager"] == "npm"
    assert status["node_packages"] == ["chalk@^5.4.0", "tsx@^4.19.0"]


def test_get_skill_env_status_requires_env_for_python_script_without_deps(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "tool.py").write_text("print('ok')\n", encoding="utf-8")

    status = get_skill_env_status("writer", workspace_dir=workspace_dir)

    assert status["kind"] == "python"
    assert status["required"] is True
    assert status["python_packages"] == []
    assert "prepare-env writer" in status["next_step"]


def test_get_skill_env_status_supports_legacy_registry_skill_path(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    legacy_dir = workspace_dir / "legacy-job"
    scripts_dir = legacy_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (legacy_dir / "SKILL.md").write_text(
        """---
name: legacy-job
description: Legacy job search
metadata:
  {
    "broodmind": {
      "runtime": {
        "python": {
          "packages": ["requests"]
        }
      }
    }
  }
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "tool.py").write_text("print('ok')\n", encoding="utf-8")
    (workspace_dir / "skills").mkdir(parents=True, exist_ok=True)
    (workspace_dir / "skills" / "registry.json").write_text(
        json.dumps(
            {
                "version": 1,
                "skills": [
                    {
                        "id": "legacy-job",
                        "name": "legacy-job",
                        "description": "Legacy job search",
                        "path": "legacy-job/SKILL.md",
                        "scope": "worker",
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    status = get_skill_env_status("legacy-job", workspace_dir=workspace_dir)

    assert status["kind"] == "python"
    assert status["required"] is True
    assert status["python_packages"] == ["requests"]


def test_prepare_skill_env_preserves_existing_env_when_rebuild_fails(tmp_path: Path, monkeypatch) -> None:
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
    env_dir = workspace_dir / ".skill-envs" / "job-search"
    bin_dir = env_dir / ("Scripts" if __import__("os").name == "nt" else "bin")
    bin_dir.mkdir(parents=True, exist_ok=True)
    python_name = "python.exe" if __import__("os").name == "nt" else "python"
    existing_python = bin_dir / python_name
    existing_python.write_text("old", encoding="utf-8")
    (env_dir / "env.json").write_text(json.dumps({"kind": "python"}), encoding="utf-8")

    def _failing_run(command, check, capture_output, text, cwd=None):
        if command[:3] == [__import__("sys").executable, "-m", "venv"]:
            staging_dir = Path(command[3])
            staging_bin = staging_dir / ("Scripts" if __import__("os").name == "nt" else "bin")
            staging_bin.mkdir(parents=True, exist_ok=True)
            (staging_bin / python_name).write_text("new", encoding="utf-8")
            return type("Completed", (), {"returncode": 0})()
        raise subprocess.CalledProcessError(1, command, "boom", "boom")

    monkeypatch.setattr("broodmind.tools.skills.runtime_envs.subprocess.run", _failing_run)

    try:
        prepare_skill_env("job-search", workspace_dir=workspace_dir)
    except subprocess.CalledProcessError:
        pass
    else:
        raise AssertionError("expected rebuild failure")

    assert existing_python.exists()
    assert existing_python.read_text(encoding="utf-8") == "old"
