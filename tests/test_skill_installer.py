from __future__ import annotations

import json
import zipfile
from pathlib import Path

from broodmind.tools.skills.installer import (
    detect_skill_install_source,
    install_skill_from_source,
    list_installed_skill_sources,
    remove_installed_skill,
    set_installed_skill_trust,
    update_installed_skill,
    verify_installed_skill,
)


def test_detect_skill_install_source_supports_clawhub_slug_and_skill_md_url() -> None:
    assert detect_skill_install_source("zanblayde/agent-commons").kind == "clawhub_slug"
    assert detect_skill_install_source("clawhub:zanblayde/agent-commons").kind == "clawhub_slug"
    assert detect_skill_install_source("https://example.com/skills/writer/SKILL.md").kind == "skill_md_url"


def test_install_skill_from_local_dir_copies_bundle_and_writes_manifest(tmp_path: Path) -> None:
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

    payload = install_skill_from_source(str(source_dir), workspace_dir=workspace_dir)

    assert payload["status"] == "installed"
    assert (workspace_dir / "skills" / "writer" / "SKILL.md").exists()
    installs = list_installed_skill_sources(workspace_dir)
    assert installs["count"] == 1
    assert installs["installs"][0]["source_kind"] == "local_dir"
    assert installs["installs"][0]["trusted"] is True
    assert installs["installs"][0]["script_scan"]["status"] == "no_scripts"


def test_install_skill_from_local_zip_extracts_bundle(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "skill.md").write_text(
        """---
name: image-lab
description: Generate images
---
""",
        encoding="utf-8",
    )
    archive_path = tmp_path / "skill.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(bundle_dir / "skill.md", arcname="image-lab/skill.md")

    payload = install_skill_from_source(str(archive_path), workspace_dir=workspace_dir)

    assert payload["skill_id"] == "image-lab"
    assert (workspace_dir / "skills" / "image-lab" / "skill.md").exists()


def test_install_skill_reports_next_step_for_python_runtime_env(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
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

    payload = install_skill_from_source(str(source_dir), workspace_dir=workspace_dir)

    assert payload["next_step"] == "uv run broodmind skill prepare-env job-search"


def test_install_skill_from_clawhub_slug_uses_download_adapter(tmp_path: Path, monkeypatch) -> None:
    workspace_dir = tmp_path / "workspace"
    archive_source_dir = tmp_path / "source"
    archive_source_dir.mkdir(parents=True)
    (archive_source_dir / "SKILL.md").write_text(
        """---
name: commons
description: Shared agent helpers
---
""",
        encoding="utf-8",
    )
    prepared_archive = tmp_path / "prepared.zip"
    with zipfile.ZipFile(prepared_archive, "w") as archive:
        archive.write(archive_source_dir / "SKILL.md", arcname="commons/SKILL.md")

    def _fake_download(slug: str, *, archive_path: Path, clawhub_site: str) -> None:
        assert slug == "zanblayde/agent-commons"
        assert clawhub_site == "https://clawhub.ai"
        archive_path.write_bytes(prepared_archive.read_bytes())

    monkeypatch.setattr(
        "broodmind.tools.skills.installer._download_clawhub_archive",
        _fake_download,
    )

    payload = install_skill_from_source(
        "zanblayde/agent-commons",
        workspace_dir=workspace_dir,
    )

    assert payload["source_kind"] == "clawhub_slug"
    installs = list_installed_skill_sources(workspace_dir)
    assert installs["installs"][0]["source"] == "zanblayde/agent-commons"
    assert installs["installs"][0]["trusted"] is False


def test_install_skill_refuses_to_overwrite_unmanaged_existing_bundle(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    existing_dir = workspace_dir / "skills" / "writer"
    existing_dir.mkdir(parents=True)
    (existing_dir / "SKILL.md").write_text(
        """---
name: writer
description: Existing local skill
---
""",
        encoding="utf-8",
    )
    source_dir = tmp_path / "writer-new"
    source_dir.mkdir(parents=True)
    (source_dir / "SKILL.md").write_text(
        """---
name: writer
description: Imported skill
---
""",
        encoding="utf-8",
    )

    try:
        install_skill_from_source(str(source_dir), workspace_dir=workspace_dir)
    except Exception as exc:
        assert "already exists locally" in str(exc)
    else:
        raise AssertionError("Expected unmanaged overwrite to be rejected")


def test_update_installed_skill_reinstalls_from_saved_source(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = tmp_path / "writer"
    source_dir.mkdir(parents=True)
    skill_file = source_dir / "SKILL.md"
    skill_file.write_text(
        """---
name: writer
description: v1
---
""",
        encoding="utf-8",
    )

    install_skill_from_source(str(source_dir), workspace_dir=workspace_dir)
    skill_file.write_text(
        """---
name: writer
description: v2
---
""",
        encoding="utf-8",
    )

    payload = update_installed_skill("writer", workspace_dir=workspace_dir)

    assert payload["status"] == "updated"
    installed_text = (workspace_dir / "skills" / "writer" / "SKILL.md").read_text(encoding="utf-8")
    assert "description: v2" in installed_text


def test_remove_installed_skill_deletes_bundle_and_manifest_entry(tmp_path: Path) -> None:
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

    payload = remove_installed_skill("writer", workspace_dir=workspace_dir)

    assert payload["status"] == "removed"
    assert not (workspace_dir / "skills" / "writer").exists()
    installs = list_installed_skill_sources(workspace_dir)
    assert installs["count"] == 0


def test_set_installed_skill_trust_updates_manifest_entry(tmp_path: Path) -> None:
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

    install_skill_from_source(str(source_dir), workspace_dir=workspace_dir, trusted=False)

    payload = set_installed_skill_trust("writer", workspace_dir=workspace_dir, trusted=True)

    assert payload["status"] == "trusted"
    installs = list_installed_skill_sources(workspace_dir)
    assert installs["installs"][0]["trusted"] is True


def test_verify_installed_skill_records_scan_findings(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    source_dir = tmp_path / "writer"
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
    (scripts_dir / "fetch.py").write_text(
        "import requests\nrequests.get('https://example.com')\n",
        encoding="utf-8",
    )

    install_skill_from_source(str(source_dir), workspace_dir=workspace_dir, trusted=False)

    installed_dir = workspace_dir / "skills" / "writer"
    if not installed_dir.exists():
        raise AssertionError("Expected installed bundle directory to exist")

    payload = verify_installed_skill("writer", workspace_dir=workspace_dir)

    assert payload["status"] == "verified"
    assert payload["script_scan"]["status"] == "review_required"
    assert payload["script_scan"]["findings"][0]["rule"] == "network_access"
