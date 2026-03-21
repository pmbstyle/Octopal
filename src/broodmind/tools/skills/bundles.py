from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_FRONTMATTER_BOUNDARY = re.compile(r"^---\s*$")
_TOP_LEVEL_FRONTMATTER_KEY = re.compile(r"^(?P<key>[A-Za-z0-9_-]+):(?:\s*(?P<value>.*))?$")
_SKILL_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


@dataclass(frozen=True)
class SkillBundleRequirements:
    bins: tuple[str, ...] = ()
    env: tuple[str, ...] = ()
    config: tuple[str, ...] = ()


@dataclass(frozen=True)
class SkillBundlePythonRuntime:
    packages: tuple[str, ...] = ()


@dataclass(frozen=True)
class SkillBundleNodeRuntime:
    packages: tuple[str, ...] = ()
    package_manager: str = "npm"


@dataclass(frozen=True)
class SkillBundleRuntime:
    python: SkillBundlePythonRuntime = field(default_factory=SkillBundlePythonRuntime)
    node: SkillBundleNodeRuntime = field(default_factory=SkillBundleNodeRuntime)


@dataclass(frozen=True)
class SkillBundleMetadata:
    skill_key: str | None = None
    primary_env: str | None = None
    homepage: str | None = None
    always: bool = False
    requires: SkillBundleRequirements = field(default_factory=SkillBundleRequirements)
    runtime: SkillBundleRuntime = field(default_factory=SkillBundleRuntime)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SkillBundle:
    id: str
    name: str
    description: str
    bundle_dir: Path
    skill_file: Path
    guidance: str
    frontmatter: dict[str, str]
    metadata: SkillBundleMetadata
    scope: str = "both"
    enabled: bool = True
    scripts_dir: Path | None = None
    references_dir: Path | None = None
    assets_dir: Path | None = None
    registry_path: str | None = None


def discover_skill_bundle_dirs(workspace_dir: Path) -> list[Path]:
    skills_root = workspace_dir.resolve() / "skills"
    if not skills_root.exists() or not skills_root.is_dir():
        return []
    bundles: list[Path] = []
    for candidate in sorted(skills_root.iterdir()):
        if not candidate.is_dir():
            continue
        skill_file = _resolve_bundle_skill_file(candidate)
        if skill_file is None:
            continue
        bundles.append(candidate.resolve())
    return bundles


def load_discovered_skill_bundles(workspace_dir: Path) -> list[SkillBundle]:
    bundles: list[SkillBundle] = []
    for bundle_dir in discover_skill_bundle_dirs(workspace_dir):
        bundle = load_skill_bundle(bundle_dir, workspace_dir=workspace_dir)
        if bundle is not None:
            bundles.append(bundle)
    return bundles


def load_skill_bundle(
    path: Path,
    *,
    workspace_dir: Path | None = None,
    registry_entry: dict[str, Any] | None = None,
) -> SkillBundle | None:
    candidate = path.resolve()
    skill_file = _resolve_bundle_skill_file(candidate)
    if skill_file is None:
        return None

    if workspace_dir is not None:
        root = workspace_dir.resolve()
        try:
            skill_file.relative_to(root)
        except ValueError:
            return None

    try:
        guidance = skill_file.read_text(encoding="utf-8")
    except Exception:
        return None

    frontmatter = parse_skill_frontmatter(guidance)
    name = _resolve_bundle_name(frontmatter, registry_entry, candidate)
    description = _resolve_bundle_description(frontmatter, registry_entry)
    if not name or not description:
        return None

    skill_id = _resolve_bundle_id(frontmatter, registry_entry, candidate)
    if not _SKILL_ID_RE.fullmatch(skill_id):
        return None

    metadata = resolve_skill_bundle_metadata(frontmatter)
    scope = _resolve_scope(frontmatter, registry_entry)
    enabled = bool(registry_entry.get("enabled", True)) if isinstance(registry_entry, dict) else True

    scripts_dir = _existing_child_dir(candidate, "scripts")
    references_dir = _existing_child_dir(candidate, "references")
    assets_dir = _existing_child_dir(candidate, "assets")

    registry_path: str | None = None
    if isinstance(registry_entry, dict):
        raw_registry_path = str(registry_entry.get("path", "")).strip()
        registry_path = raw_registry_path or None

    return SkillBundle(
        id=skill_id,
        name=name,
        description=description,
        bundle_dir=candidate,
        skill_file=skill_file,
        guidance=guidance,
        frontmatter=frontmatter,
        metadata=metadata,
        scope=scope,
        enabled=enabled,
        scripts_dir=scripts_dir,
        references_dir=references_dir,
        assets_dir=assets_dir,
        registry_path=registry_path,
    )


def parse_skill_frontmatter(content: str) -> dict[str, str]:
    lines = content.splitlines()
    if not lines or not _FRONTMATTER_BOUNDARY.match(lines[0]):
        return {}

    frontmatter_lines: list[str] = []
    for index in range(1, len(lines)):
        line = lines[index]
        if _FRONTMATTER_BOUNDARY.match(line):
            return _parse_frontmatter_lines(frontmatter_lines)
        frontmatter_lines.append(line)
    return {}


def resolve_skill_bundle_metadata(frontmatter: dict[str, str]) -> SkillBundleMetadata:
    metadata_text = frontmatter.get("metadata", "").strip()
    if not metadata_text:
        return SkillBundleMetadata()

    try:
        parsed = json.loads(metadata_text)
    except Exception:
        return SkillBundleMetadata(raw={})

    if not isinstance(parsed, dict):
        return SkillBundleMetadata(raw={})
    block = parsed.get("broodmind")
    if not isinstance(block, dict):
        block = parsed.get("openclaw")
    if not isinstance(block, dict):
        return SkillBundleMetadata(raw=parsed)

    requires = block.get("requires")
    requires_dict = requires if isinstance(requires, dict) else {}
    runtime = block.get("runtime")
    runtime_dict = runtime if isinstance(runtime, dict) else {}
    python_runtime = runtime_dict.get("python")
    python_runtime_dict = python_runtime if isinstance(python_runtime, dict) else {}
    node_runtime = runtime_dict.get("node")
    node_runtime_dict = node_runtime if isinstance(node_runtime, dict) else {}
    return SkillBundleMetadata(
        skill_key=_clean_optional_text(block.get("skillKey")),
        primary_env=_clean_optional_text(block.get("primaryEnv")),
        homepage=_clean_optional_text(block.get("homepage")),
        always=bool(block.get("always", False)),
        requires=SkillBundleRequirements(
            bins=_normalize_str_tuple(requires_dict.get("bins")),
            env=_normalize_str_tuple(requires_dict.get("env")),
            config=_normalize_str_tuple(requires_dict.get("config")),
        ),
        runtime=SkillBundleRuntime(
            python=SkillBundlePythonRuntime(
                packages=_normalize_str_tuple(python_runtime_dict.get("packages")),
            ),
            node=SkillBundleNodeRuntime(
                packages=_normalize_str_tuple(node_runtime_dict.get("packages")),
                package_manager=_normalize_package_manager(node_runtime_dict.get("packageManager")),
            ),
        ),
        raw=parsed,
    )


def _parse_frontmatter_lines(lines: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    current_key: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_key, current_lines
        if current_key is None:
            return
        parsed[current_key] = _normalize_frontmatter_value("\n".join(current_lines))
        current_key = None
        current_lines = []

    for raw_line in lines:
        match = _TOP_LEVEL_FRONTMATTER_KEY.match(raw_line)
        if match and raw_line == raw_line.lstrip():
            flush()
            current_key = match.group("key").strip().lower()
            value = match.group("value") or ""
            current_lines = [value]
            continue
        if current_key is None:
            continue
        current_lines.append(raw_line)
    flush()
    return parsed


def _normalize_frontmatter_value(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return ""
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in {'"', "'"}:
        return trimmed[1:-1]
    return trimmed


def _resolve_bundle_skill_file(path: Path) -> Path | None:
    candidate = path.resolve()
    if candidate.is_dir():
        upper = candidate / "SKILL.md"
        lower = candidate / "skill.md"
        if upper.exists() and upper.is_file():
            return upper.resolve()
        if lower.exists() and lower.is_file():
            return lower.resolve()
        return None
    if candidate.is_file() and candidate.name.lower() == "skill.md":
        return candidate
    return None


def _resolve_bundle_name(
    frontmatter: dict[str, str],
    registry_entry: dict[str, Any] | None,
    bundle_dir: Path,
) -> str:
    name = frontmatter.get("name", "").strip()
    if name:
        return name
    if isinstance(registry_entry, dict):
        raw = str(registry_entry.get("name", "")).strip()
        if raw:
            return raw
    return bundle_dir.name.strip()


def _resolve_bundle_description(
    frontmatter: dict[str, str],
    registry_entry: dict[str, Any] | None,
) -> str:
    description = frontmatter.get("description", "").strip()
    if description:
        return description
    if isinstance(registry_entry, dict):
        return str(registry_entry.get("description", "")).strip()
    return ""


def _resolve_bundle_id(
    frontmatter: dict[str, str],
    registry_entry: dict[str, Any] | None,
    bundle_dir: Path,
) -> str:
    metadata = resolve_skill_bundle_metadata(frontmatter)
    if metadata.skill_key:
        return metadata.skill_key
    frontmatter_name = frontmatter.get("name", "").strip()
    if frontmatter_name:
        return _slugify(frontmatter_name)
    if isinstance(registry_entry, dict):
        raw = str(registry_entry.get("id", "")).strip()
        if raw:
            return raw
    return _slugify(bundle_dir.name)


def _resolve_scope(
    frontmatter: dict[str, str],
    registry_entry: dict[str, Any] | None,
) -> str:
    scope = str(frontmatter.get("scope", "")).strip().lower()
    if not scope and isinstance(registry_entry, dict):
        scope = str(registry_entry.get("scope", "")).strip().lower()
    return scope if scope in {"queen", "worker", "both"} else "both"


def _existing_child_dir(bundle_dir: Path, name: str) -> Path | None:
    candidate = (bundle_dir / name).resolve()
    return candidate if candidate.exists() and candidate.is_dir() else None


def _normalize_str_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return tuple(normalized)


def _clean_optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _normalize_package_manager(value: Any) -> str:
    normalized = str(value or "npm").strip().lower() or "npm"
    return normalized if normalized in {"npm"} else "npm"


def _slugify(value: str) -> str:
    lowered = value.lower().strip()
    lowered = re.sub(r"[^a-z0-9_-]+", "_", lowered)
    lowered = re.sub(r"_+", "_", lowered).strip("_")
    return lowered
