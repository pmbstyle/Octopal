from __future__ import annotations

import json
import sys
from pathlib import Path

from octopal.tools.skills.installer import install_skill_from_source
from octopal.tools.skills.management import (
    _load_skill_inventory,
    _run_skill,
    _tool_add_skill,
    _tool_list_skills,
    _tool_remove_skill,
    _tool_run_skill_script,
    _tool_set_skill_enabled,
    _tool_use_skill,
    get_registered_skill_tools,
    get_skill_management_tools,
    list_skill_inventory,
    remove_skill,
    set_skill_enabled,
    set_skill_trust,
)


def test_load_skill_inventory_auto_discovers_bundle(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "image-lab"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: image-lab
description: Generate images from prompts
scope: worker
---

# Image Lab
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    inventory = _load_skill_inventory(workspace_dir)

    assert len(inventory) == 1
    assert inventory[0]["id"] == "image-lab"
    assert inventory[0]["source"] == "bundle"
    assert inventory[0]["auto_discovered"] is True
    assert inventory[0]["scope"] == "worker"
    assert inventory[0]["exists"] is True


def test_load_skill_inventory_prefers_registry_override(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "image-lab"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: image-lab
description: Generate images from prompts
---
""",
        encoding="utf-8",
    )
    (workspace_dir / "skills" / "registry.json").write_text(
        json.dumps(
            {
                "version": 1,
                "skills": [
                    {
                        "id": "image-lab",
                        "name": "Image Lab Override",
                        "description": "Registry override wins",
                        "path": "skills/image-lab/SKILL.md",
                        "scope": "octo",
                        "enabled": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    inventory = _load_skill_inventory(workspace_dir)

    assert len(inventory) == 1
    assert inventory[0]["id"] == "image-lab"
    assert inventory[0]["source"] == "registry"
    assert inventory[0]["name"] == "image-lab"
    assert inventory[0]["description"] == "Generate images from prompts"
    assert inventory[0]["scope"] == "octo"
    assert inventory[0]["enabled"] is False


def test_registry_skill_file_path_still_detects_bundle_scripts(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "job-search"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: job-search
description: Search jobs
scope: worker
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "jobspy.py").write_text("print('ok')\n", encoding="utf-8")
    (workspace_dir / "skills" / "registry.json").write_text(
        json.dumps(
            {
                "version": 1,
                "skills": [
                    {
                        "id": "job-search",
                        "name": "job-search",
                        "description": "Search jobs",
                        "path": "skills/job-search/SKILL.md",
                        "scope": "worker",
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    inventory = _load_skill_inventory(workspace_dir)

    assert len(inventory) == 1
    assert inventory[0]["id"] == "job-search"
    assert inventory[0]["has_scripts"] is True
    assert inventory[0]["scripts_dir"].endswith("skills/job-search/scripts") or inventory[0]["scripts_dir"].endswith(
        "skills\\job-search\\scripts"
    )


def test_list_skill_inventory_uses_explicit_workspace_for_runtime_status(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.delenv("OCTOPAL_WORKSPACE_DIR", raising=False)

    payload = list_skill_inventory(workspace_dir)

    assert payload["skills"][0]["runtime_kind"] == "python"
    assert payload["skills"][0]["runtime_required"] is True


def test_list_skill_inventory_reports_unsupported_mixed_runtime(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "researcher"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: researcher
description: Research helper
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "tool.py").write_text("print('ok')\n", encoding="utf-8")
    (skill_dir / "package.json").write_text('{"dependencies":{"left-pad":"1.3.0"}}\n', encoding="utf-8")

    payload = list_skill_inventory(workspace_dir)

    skill = payload["skills"][0]
    assert skill["runtime_kind"] == "mixed"
    assert skill["runtime_status"] == "unsupported"
    assert "mixed python and node runtimes" in skill["runtime_reason"]
    assert skill["status"] == "not_ready"
    assert "mixed python and node runtimes" in skill["reasons"][0]


def test_load_skill_inventory_keeps_legacy_registry_skill(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    legacy_dir = workspace_dir / "legacy"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "skill.md").write_text("# Legacy\n", encoding="utf-8")
    (workspace_dir / "skills").mkdir(parents=True, exist_ok=True)
    (workspace_dir / "skills" / "registry.json").write_text(
        json.dumps(
            {
                "version": 1,
                "skills": [
                    {
                        "id": "legacy_tooling",
                        "name": "Legacy Tooling",
                        "description": "Legacy registry entry",
                        "path": "legacy/skill.md",
                        "scope": "both",
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    inventory = _load_skill_inventory(workspace_dir)

    assert len(inventory) == 1
    assert inventory[0]["id"] == "legacy_tooling"
    assert inventory[0]["source"] == "registry"
    assert inventory[0]["exists"] is True


def test_add_skill_can_infer_name_and_description_from_skill_file(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = json.loads(_tool_add_skill({"path": "skills/writer"}, {}))

    assert payload["status"] == "added"
    listed = json.loads(_tool_list_skills({}, {}))
    assert listed["count"] == 1
    assert listed["skills"][0]["name"] == "writer"
    assert listed["skills"][0]["description"] == "Helps write copy"


def test_registered_skill_tools_include_auto_discovered_enabled_bundle(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    tools = get_registered_skill_tools()

    assert [tool.name for tool in tools] == ["skill_writer"]
    assert "Helps write copy" in tools[0].description


def test_run_skill_payload_includes_usage_hints_for_script_skill(tmp_path: Path, monkeypatch) -> None:
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
    (scripts_dir / "noop.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    inventory = _load_skill_inventory(workspace_dir)
    payload = json.loads(_run_skill(inventory[0], {}, {"worker": object()}))

    assert payload["scripts_available"] is True
    assert "not MCP servers" in payload["usage_hint"]
    assert "run_skill_script" in payload["script_usage_hint"]


def test_skill_management_tools_include_run_skill_script() -> None:
    tools = get_skill_management_tools()

    assert "run_skill_script" in [tool.name for tool in tools]
    assert "use_skill" in [tool.name for tool in tools]
    assert "set_skill_enabled" in [tool.name for tool in tools]


def test_use_skill_reads_guidance_by_id(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---

# Writer
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = json.loads(_tool_use_skill({"skill_id": "writer"}, {"worker": object()}))

    assert payload["skill_id"] == "writer"
    assert "Skills are internal Octopal tools" in payload["usage_hint"]
    assert "# Writer" in payload["guidance"]


def test_set_skill_enabled_disables_auto_discovered_bundle(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---

# Writer
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    disabled = set_skill_enabled("writer", workspace_dir=workspace_dir, enabled=False)

    assert disabled["status"] == "disabled"
    listed = list_skill_inventory(workspace_dir)
    assert listed["skills"][0]["enabled"] is False
    assert listed["skills"][0]["status"] == "disabled"
    assert get_registered_skill_tools() == []
    assert "is disabled" in _tool_use_skill({"skill_id": "writer"}, {"worker": object()})

    enabled = set_skill_enabled("writer", workspace_dir=workspace_dir, enabled=True)

    assert enabled["status"] == "enabled"
    assert list_skill_inventory(workspace_dir)["skills"][0]["enabled"] is True
    assert [tool.name for tool in get_registered_skill_tools()] == ["skill_writer"]


def test_registered_skill_tool_rechecks_enabled_state_after_disable(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
---

# Writer
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    tools = get_registered_skill_tools()
    handler = next(tool.handler for tool in tools if tool.name == "skill_writer")

    set_skill_enabled("writer", workspace_dir=workspace_dir, enabled=False)

    assert handler({}, {"worker": object()}) == "skill error: skill 'writer' is disabled."


def test_tool_set_skill_enabled_updates_registry_override(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = json.loads(_tool_set_skill_enabled({"id": "writer", "enabled": False}, {}))

    assert payload["status"] == "disabled"
    assert payload["enabled"] is False
    assert list_skill_inventory(workspace_dir)["skills"][0]["enabled"] is False


def test_remove_installed_skill_cleans_disable_override(tmp_path: Path) -> None:
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
    install_skill_from_source(str(source_dir), workspace_dir=workspace_dir)
    set_skill_enabled("writer", workspace_dir=workspace_dir, enabled=False)

    payload = remove_skill("writer", workspace_dir=workspace_dir)

    assert payload["installer_managed"] is True
    assert payload["removed_registry_override"] is True
    assert list_skill_inventory(workspace_dir)["skills"] == []


def test_run_skill_script_executes_python_from_bundle_scripts_dir(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    worker_dir = workspace_dir / "workers" / "copy-worker"
    worker_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
scope: worker
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "echo_args.py").write_text(
        """from __future__ import annotations
import json
import os
import sys

payload = {
    "cwd": os.getcwd(),
    "args": sys.argv[1:],
}
print(json.dumps(payload))
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))
    monkeypatch.setattr(
        "octopal.tools.skills.management.get_skill_env_status",
        lambda skill_id, workspace_dir: {
            "skill_id": skill_id,
            "kind": "python",
            "required": True,
            "recommended": True,
            "prepared": True,
            "status": "prepared",
            "reason": "",
            "manifest_path": "",
            "next_step": "",
            "python_packages": [],
            "node_packages": [],
            "package_manager": "",
        },
    )
    monkeypatch.setattr(
        "octopal.tools.skills.management.resolve_skill_runtime_execution",
        lambda skill_id, workspace_dir, script_path, explicit_runner: {
            "runner": [sys.executable, str(script_path)],
            "env": None,
        },
    )

    raw = _tool_run_skill_script(
        {"skill_id": "writer", "script": "echo_args.py", "args": ["hello"], "workdir": "."},
        {"base_dir": worker_dir, "worker": object()},
    )
    payload = json.loads(raw)
    stdout_payload = json.loads(payload["stdout"])

    assert payload["returncode"] == 0
    assert Path(payload["runner"]).resolve() == Path(sys.executable).resolve()
    assert stdout_payload["args"] == ["hello"]
    assert Path(stdout_payload["cwd"]).resolve() == worker_dir.resolve()


def test_run_skill_script_rejects_path_escape(tmp_path: Path, monkeypatch) -> None:
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
    (scripts_dir / "ok.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))
    monkeypatch.setattr(
        "octopal.tools.skills.management.get_skill_env_status",
        lambda skill_id, workspace_dir: {
            "skill_id": skill_id,
            "kind": "python",
            "required": True,
            "recommended": True,
            "prepared": True,
            "status": "prepared",
            "reason": "",
            "manifest_path": "",
            "next_step": "",
            "python_packages": [],
            "node_packages": [],
            "package_manager": "",
        },
    )

    result = _tool_run_skill_script(
        {"skill_id": "writer", "script": "../outside.py"},
        {"base_dir": workspace_dir, "worker": object()},
    )

    assert "must stay inside the skill scripts directory" in result


def test_list_skills_reports_not_ready_requirements(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "image-lab"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: image-lab
description: Generate images
metadata:
  {
    "octopal": {
      "primaryEnv": "OPENAI_API_KEY",
      "requires": {
        "bins": ["definitely_missing_binary"],
        "env": ["OPENAI_API_KEY"]
      }
    }
  }
---
""",
        encoding="utf-8",
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = json.loads(_tool_list_skills({}, {}))

    assert payload["skills"][0]["status"] == "not_ready"
    assert payload["skills"][0]["ready"] is False
    assert payload["skills"][0]["missing_bins"] == ["definitely_missing_binary"]
    assert payload["skills"][0]["missing_env"] == ["OPENAI_API_KEY"]


def test_list_skills_reports_untrusted_installed_scripts(tmp_path: Path, monkeypatch) -> None:
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
    (scripts_dir / "noop.py").write_text("print('ok')\n", encoding="utf-8")
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
                        "script_scan": {
                            "status": "clean",
                            "scanned_at": "2026-03-21T10:00:00+00:00",
                            "file_count": 1,
                            "files": [],
                            "findings": [],
                        },
                        "path": str(skill_dir / "SKILL.md"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = json.loads(_tool_list_skills({}, {}))

    assert payload["skills"][0]["installer_managed"] is True
    assert payload["skills"][0]["trusted"] is False
    assert payload["skills"][0]["status"] == "not_ready"
    assert "not trusted yet" in payload["skills"][0]["reasons"][0]
    assert payload["skills"][0]["scan_status"] == "clean"


def test_list_skills_reports_untrusted_local_scripts(tmp_path: Path, monkeypatch) -> None:
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
    (scripts_dir / "noop.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    set_skill_trust("writer", workspace_dir=workspace_dir, trusted=False)
    payload = json.loads(_tool_list_skills({}, {}))

    assert payload["skills"][0]["installer_managed"] is False
    assert payload["skills"][0]["trusted"] is False
    assert payload["skills"][0]["status"] == "not_ready"
    assert any("not trusted yet" in reason for reason in payload["skills"][0]["reasons"])


def test_remove_skill_deletes_local_bundle(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = remove_skill("writer", workspace_dir=workspace_dir)

    assert payload["status"] == "removed"
    assert payload["installer_managed"] is False
    assert not skill_dir.exists()


def test_tool_remove_skill_deletes_auto_discovered_local_bundle(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    payload = json.loads(_tool_remove_skill({"id": "writer"}, {}))

    assert payload["status"] == "removed"
    assert payload["skill_id"] == "writer"
    assert not skill_dir.exists()


def test_run_skill_script_blocks_when_skill_is_not_ready(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "image-lab"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: image-lab
description: Generate images
scope: worker
metadata:
  {
    "octopal": {
      "requires": {
        "env": ["OPENAI_API_KEY"]
      }
    }
  }
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "noop.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    result = _tool_run_skill_script(
        {"skill_id": "image-lab", "script": "noop.py"},
        {"base_dir": workspace_dir / "workers", "worker": object()},
    )

    assert "is not ready" in result
    assert "OPENAI_API_KEY" in result


def test_run_skill_script_blocks_untrusted_installed_scripts(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
scope: worker
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "noop.py").write_text("print('ok')\n", encoding="utf-8")
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
                        "script_scan": {
                            "status": "review_required",
                            "scanned_at": "2026-03-21T10:00:00+00:00",
                            "file_count": 1,
                            "files": [],
                            "findings": [{"path": "scripts/noop.py", "rule": "process_execution"}],
                        },
                        "path": str(skill_dir / "SKILL.md"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    result = _tool_run_skill_script(
        {"skill_id": "writer", "script": "noop.py"},
        {"base_dir": workspace_dir / "workers", "worker": object()},
    )

    assert "is not ready" in result
    assert "not trusted yet" in result


def test_run_skill_script_blocks_when_runtime_env_is_required(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "job-search"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: job-search
description: Search jobs
scope: worker
metadata:
  {
    "octopal": {
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
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    result = _tool_run_skill_script(
        {"skill_id": "job-search", "script": "jobspy.py"},
        {"base_dir": workspace_dir / "workers", "worker": object()},
    )

    assert "runtime env is not prepared" in result
    assert "prepare-env job-search" in result


def test_run_skill_script_blocks_python_script_without_prepared_env(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    skill_dir = workspace_dir / "skills" / "writer"
    scripts_dir = skill_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        """---
name: writer
description: Helps write copy
scope: worker
---
""",
        encoding="utf-8",
    )
    (scripts_dir / "tool.py").write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setenv("OCTOPAL_WORKSPACE_DIR", str(workspace_dir))

    result = _tool_run_skill_script(
        {"skill_id": "writer", "script": "tool.py"},
        {"base_dir": workspace_dir / "workers", "worker": object()},
    )

    assert "runtime env is not prepared" in result
    assert "prepare-env writer" in result
