from __future__ import annotations

from pathlib import Path

from broodmind.tools.skills.bundles import (
    SkillBundleMetadata,
    discover_skill_bundle_dirs,
    load_discovered_skill_bundles,
    load_skill_bundle,
    parse_skill_frontmatter,
    resolve_skill_bundle_metadata,
)


def test_parse_skill_frontmatter_reads_basic_fields() -> None:
    content = """---
name: hello-world
description: Test bundle
scope: worker
---

# Hello
"""
    parsed = parse_skill_frontmatter(content)

    assert parsed["name"] == "hello-world"
    assert parsed["description"] == "Test bundle"
    assert parsed["scope"] == "worker"


def test_parse_skill_frontmatter_preserves_multiline_metadata_block() -> None:
    content = """---
name: image-lab
description: Generate images
metadata:
  {
    "openclaw": {
      "primaryEnv": "OPENAI_API_KEY",
      "requires": { "bins": ["python3"], "env": ["OPENAI_API_KEY"] }
    }
  }
---
"""
    parsed = parse_skill_frontmatter(content)
    metadata = resolve_skill_bundle_metadata(parsed)

    assert metadata.primary_env == "OPENAI_API_KEY"
    assert metadata.requires.bins == ("python3",)
    assert metadata.requires.env == ("OPENAI_API_KEY",)


def test_load_skill_bundle_detects_bundle_resources(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    bundle_dir = workspace_dir / "skills" / "image-lab"
    (bundle_dir / "scripts").mkdir(parents=True)
    (bundle_dir / "references").mkdir()
    (bundle_dir / "assets").mkdir()
    (bundle_dir / "SKILL.md").write_text(
        """---
name: image-lab
description: Generate or edit images
---

# Image Lab
""",
        encoding="utf-8",
    )

    bundle = load_skill_bundle(bundle_dir, workspace_dir=workspace_dir)

    assert bundle is not None
    assert bundle.id == "image-lab"
    assert bundle.name == "image-lab"
    assert bundle.description == "Generate or edit images"
    assert bundle.scripts_dir == (bundle_dir / "scripts").resolve()
    assert bundle.references_dir == (bundle_dir / "references").resolve()
    assert bundle.assets_dir == (bundle_dir / "assets").resolve()


def test_load_skill_bundle_can_fall_back_to_registry_fields(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    bundle_dir = workspace_dir / "skills" / "legacy_skill"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "skill.md").write_text("# Legacy Skill\n", encoding="utf-8")

    bundle = load_skill_bundle(
        bundle_dir,
        workspace_dir=workspace_dir,
        registry_entry={
            "id": "legacy_skill",
            "name": "Legacy Skill",
            "description": "Legacy registry-backed skill",
            "scope": "queen",
            "enabled": False,
            "path": "skills/legacy_skill/skill.md",
        },
    )

    assert bundle is not None
    assert bundle.id == "legacy_skill"
    assert bundle.name == "Legacy Skill"
    assert bundle.description == "Legacy registry-backed skill"
    assert bundle.scope == "queen"
    assert bundle.enabled is False


def test_load_skill_bundle_rejects_path_outside_workspace(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    external_dir = tmp_path / "external-skill"
    external_dir.mkdir()
    (external_dir / "SKILL.md").write_text(
        """---
name: external
description: outside workspace
---
""",
        encoding="utf-8",
    )

    bundle = load_skill_bundle(external_dir, workspace_dir=workspace_dir)

    assert bundle is None


def test_discover_and_load_skill_bundles_only_picks_valid_dirs(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"
    valid_dir = workspace_dir / "skills" / "valid-one"
    invalid_dir = workspace_dir / "skills" / "empty-dir"
    valid_dir.mkdir(parents=True)
    invalid_dir.mkdir(parents=True)
    (valid_dir / "SKILL.md").write_text(
        """---
name: valid-one
description: Valid bundle
---
""",
        encoding="utf-8",
    )

    discovered = discover_skill_bundle_dirs(workspace_dir)
    loaded = load_discovered_skill_bundles(workspace_dir)

    assert discovered == [valid_dir.resolve()]
    assert [bundle.id for bundle in loaded] == ["valid-one"]


def test_resolve_skill_bundle_metadata_handles_missing_or_invalid_json() -> None:
    assert resolve_skill_bundle_metadata({}) == SkillBundleMetadata()
    assert resolve_skill_bundle_metadata({"metadata": "not-json"}) == SkillBundleMetadata(raw={})
