from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import structlog

from broodmind.tools.filesystem.path_safety import WorkspacePathError, resolve_workspace_path
from broodmind.tools.skills.bundles import (
    SkillBundle,
    discover_skill_bundle_dirs,
    load_skill_bundle,
)
from broodmind.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)

_SKILL_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_DEFAULT_MAX_CHARS = 16_000
_MAX_CHARS_LIMIT = 200_000
_REGISTRY_VERSION = 1
_DEFAULT_SCRIPT_TIMEOUT_SECONDS = 60
_MAX_SCRIPT_TIMEOUT_SECONDS = 600
_MAX_SCRIPT_OUTPUT_CHARS = 8_000


def ensure_skills_layout(workspace_dir: Path | None = None) -> Path:
    root = workspace_dir.resolve() if workspace_dir else _workspace_root()
    skills_dir = root / "skills"
    skills_dir.mkdir(parents=True, exist_ok=True)
    path = _registry_path(root)
    if not path.exists():
        _write_registry(root, {"version": _REGISTRY_VERSION, "skills": []})
    return path


def get_skill_management_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="list_skills",
            description="List internal skills from registry and auto-discovered skill bundles.",
            parameters={
                "type": "object",
                "properties": {
                    "include_disabled": {
                        "type": "boolean",
                        "description": "Include disabled skills in the result (default false).",
                    }
                },
                "additionalProperties": False,
            },
            permission="skill_manage",
            handler=_tool_list_skills,
        ),
        ToolSpec(
            name="add_skill",
            description="Register a new internal skill from a SKILL.md path.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Skill id (optional; inferred from name/path when omitted)."},
                    "name": {"type": "string", "description": "Human-friendly skill name."},
                    "description": {"type": "string", "description": "Short skill description."},
                    "path": {"type": "string", "description": "Path to SKILL.md (or its containing directory)."},
                    "scope": {
                        "type": "string",
                        "description": "Where the skill should be available.",
                        "enum": ["queen", "worker", "both"],
                    },
                    "enabled": {"type": "boolean", "description": "Whether the skill is enabled (default true)."},
                },
                "required": ["path"],
                "additionalProperties": False,
            },
            permission="skill_manage",
            handler=_tool_add_skill,
        ),
        ToolSpec(
            name="remove_skill",
            description="Remove a skill from the internal registry by id.",
            parameters={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Skill id to remove."},
                },
                "required": ["id"],
                "additionalProperties": False,
            },
            permission="skill_manage",
            handler=_tool_remove_skill,
        ),
        ToolSpec(
            name="run_skill_script",
            description="Run a script from a skill bundle scripts/ directory without invoking a shell.",
            parameters={
                "type": "object",
                "properties": {
                    "skill_id": {"type": "string", "description": "Skill id to execute from."},
                    "script": {"type": "string", "description": "Relative path inside the skill scripts/ directory."},
                    "args": {
                        "type": "array",
                        "description": "Optional string arguments passed to the script.",
                        "items": {"type": "string"},
                    },
                    "workdir": {
                        "type": "string",
                        "description": "Optional working directory relative to current base_dir or absolute within workspace.",
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Execution timeout in seconds (default 60, max 600).",
                    },
                    "runner": {
                        "type": "string",
                        "description": "Optional explicit runner.",
                        "enum": ["python", "python3", "node", "bash", "sh", "powershell", "pwsh", "direct"],
                    },
                },
                "required": ["skill_id", "script"],
                "additionalProperties": False,
            },
            permission="skill_exec",
            handler=_tool_run_skill_script,
        ),
    ]


def get_registered_skill_tools() -> list[ToolSpec]:
    workspace_dir = _workspace_root()
    skills = _load_skill_inventory(workspace_dir)
    tools: list[ToolSpec] = []
    for raw in skills:
        if not bool(raw.get("enabled", True)):
            continue
        skill_id = str(raw.get("id", "")).strip()
        if not _SKILL_ID_RE.fullmatch(skill_id):
            continue
        tool_name = f"skill_{skill_id}"
        if not bool(raw.get("exists", False)):
            logger.warning("Skipping skill tool because SKILL.md is missing", skill_id=skill_id)
            continue
        name = str(raw.get("name", skill_id)).strip() or skill_id
        description = str(raw.get("description", "")).strip()
        scope = str(raw.get("scope", "both")).strip().lower() or "both"

        def _handler(args: dict[str, Any], ctx: dict[str, Any], skill_data: dict[str, Any] = raw) -> str:
            return _run_skill(skill_data, args, ctx)

        tools.append(
            ToolSpec(
                name=tool_name,
                description=(
                    f"Apply internal skill '{name}'. "
                    f"{description}" + (f" Scope: {scope}." if scope in {"queen", "worker", "both"} else "")
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "task": {
                            "type": "string",
                            "description": "Optional task context to pair with the skill guidance.",
                        },
                        "input": {
                            "type": "object",
                            "description": "Optional structured input context for this skill run.",
                        },
                        "max_chars": {
                            "type": "integer",
                            "description": "Max characters to return from SKILL.md (200-200000).",
                        },
                    },
                    "additionalProperties": False,
                },
                permission="skill_use",
                handler=_handler,
            )
        )
    return tools


def _tool_list_skills(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    workspace_dir = _workspace_root()
    include_disabled = bool(args.get("include_disabled", False))
    listed: list[dict[str, Any]] = []
    for raw in _load_skill_inventory(workspace_dir):
        enabled = bool(raw.get("enabled", True))
        if not include_disabled and not enabled:
            continue
        listed.append(
            {
                "id": str(raw.get("id", "")),
                "name": str(raw.get("name", "")),
                "description": str(raw.get("description", "")),
                "path": str(raw.get("path", "")),
                "scope": str(raw.get("scope", "both")),
                "enabled": enabled,
                "exists": bool(raw.get("exists", False)),
                "source": str(raw.get("source", "registry")),
                "auto_discovered": bool(raw.get("auto_discovered", False)),
                "installer_managed": bool(raw.get("installer_managed", False)),
                "trusted": bool(raw.get("trusted", True)),
                "has_scripts": bool(raw.get("has_scripts", False)),
                "installed_source": str(raw.get("installed_source", "")),
                "installed_source_kind": str(raw.get("installed_source_kind", "")),
                **_evaluate_skill_status(raw),
            }
        )
    payload = {
        "count": len(listed),
        "registry_path": str(_registry_path(workspace_dir)),
        "skills": listed,
    }
    return json.dumps(payload, ensure_ascii=False)


def _tool_add_skill(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    workspace_dir = _workspace_root()
    path_raw = str(args.get("path", "")).strip()
    scope = str(args.get("scope", "both")).strip().lower() or "both"
    enabled = bool(args.get("enabled", True))

    if not path_raw:
        return "add_skill error: path is required."
    if scope not in {"queen", "worker", "both"}:
        return "add_skill error: scope must be one of queen, worker, both."

    resolved_skill = _resolve_skill_file(workspace_dir, path_raw)
    if resolved_skill is None:
        return "add_skill error: skill path not found, invalid, or outside workspace."

    candidate_entry = {
        "id": str(args.get("id", "")).strip(),
        "name": str(args.get("name", "")).strip(),
        "description": str(args.get("description", "")).strip(),
        "scope": scope,
        "enabled": enabled,
        "path": resolved_skill.relative_to(workspace_dir).as_posix(),
    }
    bundle = load_skill_bundle(resolved_skill, workspace_dir=workspace_dir, registry_entry=candidate_entry)
    if bundle is None:
        return "add_skill error: SKILL.md is invalid or incomplete. Name/description may be missing."

    skill_id = bundle.id
    if not _SKILL_ID_RE.fullmatch(skill_id):
        return "add_skill error: id must match ^[a-z0-9][a-z0-9_-]*$."

    registry = _load_registry(workspace_dir)
    skills = [item for item in registry.get("skills", []) if isinstance(item, dict)]
    existing_idx = next((i for i, item in enumerate(skills) if str(item.get("id", "")) == skill_id), None)
    record = {
        "id": bundle.id,
        "name": bundle.name,
        "description": bundle.description,
        "path": bundle.skill_file.relative_to(workspace_dir).as_posix(),
        "scope": bundle.scope,
        "enabled": bundle.enabled,
    }
    if existing_idx is None:
        skills.append(record)
        action = "added"
    else:
        skills[existing_idx] = record
        action = "updated"

    registry["skills"] = skills
    _write_registry(workspace_dir, registry)

    return json.dumps(
        {
            "status": action,
            "id": skill_id,
            "path": record["path"],
            "registry_path": str(_registry_path(workspace_dir)),
            "message": f"Skill '{skill_id}' {action} successfully.",
        },
        ensure_ascii=False,
    )


def _tool_remove_skill(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    workspace_dir = _workspace_root()
    skill_id = str(args.get("id", "")).strip()
    if not _SKILL_ID_RE.fullmatch(skill_id):
        return "remove_skill error: id must match ^[a-z0-9][a-z0-9_-]*$."

    registry = _load_registry(workspace_dir)
    skills = [item for item in registry.get("skills", []) if isinstance(item, dict)]
    kept = [item for item in skills if str(item.get("id", "")) != skill_id]
    if len(kept) == len(skills):
        return f"remove_skill error: skill '{skill_id}' not found."

    registry["skills"] = kept
    _write_registry(workspace_dir, registry)
    return json.dumps(
        {
            "status": "removed",
            "id": skill_id,
            "registry_path": str(_registry_path(workspace_dir)),
            "message": f"Skill '{skill_id}' removed from registry.",
        },
        ensure_ascii=False,
    )


def _tool_run_skill_script(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    workspace_dir = _workspace_root()
    skill_id = str(args.get("skill_id", "")).strip()
    if not _SKILL_ID_RE.fullmatch(skill_id):
        return "run_skill_script error: skill_id must match ^[a-z0-9][a-z0-9_-]*$."

    skill_data = next((item for item in _load_skill_inventory(workspace_dir) if str(item.get("id", "")) == skill_id), None)
    if skill_data is None:
        return f"run_skill_script error: skill '{skill_id}' not found."
    if not bool(skill_data.get("enabled", True)):
        return f"run_skill_script error: skill '{skill_id}' is disabled."
    readiness = _evaluate_skill_status(skill_data)
    if not bool(readiness.get("ready", False)):
        reasons = readiness.get("reasons", [])
        reason_suffix = f" Reasons: {', '.join(str(item) for item in reasons)}" if reasons else ""
        return f"run_skill_script error: skill '{skill_id}' is not ready.{reason_suffix}"

    scope = str(skill_data.get("scope", "both")).strip().lower() or "both"
    caller_scope = _caller_scope(ctx)
    if scope == "queen" and caller_scope == "worker":
        return "run_skill_script error: this skill is scoped to queen only."
    if scope == "worker" and caller_scope == "queen":
        return "run_skill_script error: this skill is scoped to worker only."

    scripts_dir_raw = str(skill_data.get("scripts_dir", "")).strip()
    if not scripts_dir_raw:
        return f"run_skill_script error: skill '{skill_id}' has no scripts directory."
    scripts_dir = Path(scripts_dir_raw).resolve()
    if not scripts_dir.exists() or not scripts_dir.is_dir():
        return f"run_skill_script error: scripts directory is missing for '{skill_id}'."

    script_raw = str(args.get("script", "")).strip()
    if not script_raw:
        return "run_skill_script error: script is required."
    try:
        script_path = _resolve_skill_script_path(scripts_dir, script_raw)
    except ValueError as exc:
        return f"run_skill_script error: {exc}"

    workdir = _resolve_skill_workdir(workspace_dir, ctx, args)
    if isinstance(workdir, str):
        return workdir

    args_list = args.get("args")
    script_args = _normalize_script_args(args_list)
    if script_args is None:
        return "run_skill_script error: args must be an array of strings."

    runner = str(args.get("runner", "")).strip().lower()
    try:
        command = _build_skill_script_command(script_path, runner)
    except ValueError as exc:
        return f"run_skill_script error: {exc}"

    timeout_seconds = _bounded_int(
        args.get("timeout_seconds"),
        default=_DEFAULT_SCRIPT_TIMEOUT_SECONDS,
        low=1,
        high=_MAX_SCRIPT_TIMEOUT_SECONDS,
    )

    try:
        result = subprocess.run(
            command + script_args,
            cwd=str(workdir),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            shell=False,
        )
    except subprocess.TimeoutExpired:
        return f"run_skill_script error: script timed out after {timeout_seconds}s."
    except FileNotFoundError as exc:
        missing = exc.filename or command[0]
        return f"run_skill_script error: runner not found: {missing}"
    except Exception as exc:
        return f"run_skill_script error: {exc}"

    payload = {
        "skill_id": skill_id,
        "script": script_path.relative_to(scripts_dir).as_posix(),
        "runner": command[0],
        "command": command + script_args,
        "workdir": str(workdir),
        "returncode": int(result.returncode),
        "stdout": result.stdout[:_MAX_SCRIPT_OUTPUT_CHARS],
        "stderr": result.stderr[:_MAX_SCRIPT_OUTPUT_CHARS],
        "stdout_truncated": len(result.stdout) > _MAX_SCRIPT_OUTPUT_CHARS,
        "stderr_truncated": len(result.stderr) > _MAX_SCRIPT_OUTPUT_CHARS,
    }
    return json.dumps(payload, ensure_ascii=False)


def _run_skill(skill_data: dict[str, Any], args: dict[str, Any], ctx: dict[str, Any]) -> str:
    workspace_dir = _workspace_root()
    scope = str(skill_data.get("scope", "both")).strip().lower() or "both"
    caller_scope = _caller_scope(ctx)
    if scope == "queen" and caller_scope == "worker":
        return "skill error: this skill is scoped to queen only."
    if scope == "worker" and caller_scope == "queen":
        return "skill error: this skill is scoped to worker only."

    skill_path = _resolve_registered_skill_path(workspace_dir, skill_data)
    if skill_path is None or not skill_path.exists():
        return f"skill error: missing SKILL.md for '{skill_data.get('id', '<unknown>')}'."

    max_chars = _bounded_int(args.get("max_chars"), default=_DEFAULT_MAX_CHARS, low=200, high=_MAX_CHARS_LIMIT)
    task = str(args.get("task", "")).strip()
    input_payload = args.get("input")

    try:
        content = skill_path.read_text(encoding="utf-8")
    except Exception as exc:
        return f"skill error: failed to read SKILL.md: {exc}"

    truncated = False
    if len(content) > max_chars:
        content = content[:max_chars]
        truncated = True

    payload = {
        "skill_id": str(skill_data.get("id", "")),
        "name": str(skill_data.get("name", "")),
        "description": str(skill_data.get("description", "")),
        "scope": scope,
        "path": str(skill_data.get("path", "")),
        "source": str(skill_data.get("source", "registry")),
        "installer_managed": bool(skill_data.get("installer_managed", False)),
        "trusted": bool(skill_data.get("trusted", True)),
        "has_scripts": bool(skill_data.get("has_scripts", False)),
        "installed_source": str(skill_data.get("installed_source", "")),
        "installed_source_kind": str(skill_data.get("installed_source_kind", "")),
        **_evaluate_skill_status(skill_data),
        "task": task,
        "input": input_payload if isinstance(input_payload, (dict, list, str, int, float, bool)) else None,
        "truncated": truncated,
        "guidance": content,
    }
    return json.dumps(payload, ensure_ascii=False)


def _caller_scope(ctx: dict[str, Any]) -> str:
    if "queen" in ctx:
        return "queen"
    if "worker" in ctx:
        return "worker"
    return "unknown"


def _workspace_root() -> Path:
    raw = os.getenv("BROODMIND_WORKSPACE_DIR", "").strip()
    if raw:
        return Path(raw).resolve()
    cwd = Path.cwd().resolve()
    if cwd.name == "workers":
        return cwd.parent
    if cwd.parent.name == "workers":
        return cwd.parent.parent
    if (cwd / "workers").exists():
        return cwd
    default_candidate = Path("workspace").resolve()
    if (default_candidate / "workers").exists():
        return default_candidate
    return default_candidate


def _registry_path(workspace_dir: Path) -> Path:
    return workspace_dir / "skills" / "registry.json"


def _load_registry(workspace_dir: Path) -> dict[str, Any]:
    path = ensure_skills_layout(workspace_dir)
    if not path.exists():
        return {"version": _REGISTRY_VERSION, "skills": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to parse skills registry, using empty registry", path=str(path), error=str(exc))
        return {"version": _REGISTRY_VERSION, "skills": []}
    if not isinstance(payload, dict):
        return {"version": _REGISTRY_VERSION, "skills": []}
    skills = payload.get("skills")
    if not isinstance(skills, list):
        payload["skills"] = []
    payload.setdefault("version", _REGISTRY_VERSION)
    return payload


def _write_registry(workspace_dir: Path, payload: dict[str, Any]) -> None:
    path = _registry_path(workspace_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        tmp.replace(path)
    except PermissionError:
        # Windows can transiently deny os.replace when target is locked.
        path.write_text(text, encoding="utf-8")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def _resolve_skill_file(workspace_dir: Path, path_raw: str) -> Path | None:
    candidate = Path(path_raw)
    if not candidate.is_absolute():
        candidate = workspace_dir / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(workspace_dir)
    except ValueError:
        return None
    if candidate.is_dir():
        preferred = candidate / "SKILL.md"
        fallback = candidate / "skill.md"
        if preferred.exists():
            candidate = preferred
        elif fallback.exists():
            candidate = fallback
        else:
            candidate = preferred
    if candidate.name.lower() != "skill.md":
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def _resolve_registered_skill_path(workspace_dir: Path, skill_data: dict[str, Any]) -> Path | None:
    raw = str(skill_data.get("path", "")).strip()
    if not raw:
        return None
    return _resolve_skill_file(workspace_dir, raw)


def _skill_path_exists(workspace_dir: Path, skill_data: dict[str, Any]) -> bool:
    return _resolve_registered_skill_path(workspace_dir, skill_data) is not None


def _load_skill_inventory(workspace_dir: Path) -> list[dict[str, Any]]:
    registry = _load_registry(workspace_dir)
    installed_manifest = _load_installed_manifest(workspace_dir)
    installed_by_id = {
        str(item.get("skill_id", "")).strip(): item
        for item in installed_manifest.get("installs", [])
        if isinstance(item, dict) and str(item.get("skill_id", "")).strip()
    }
    registry_skills = [item for item in registry.get("skills", []) if isinstance(item, dict)]
    inventory_by_id: dict[str, dict[str, Any]] = {}

    for raw in registry_skills:
        bundle = _load_registry_bundle(workspace_dir, raw)
        skill_id = str(raw.get("id", "")).strip()
        if bundle is not None:
            inventory_by_id[bundle.id] = _merge_installed_metadata(
                _skill_record_from_bundle(
                    workspace_dir,
                    bundle,
                    source="registry",
                    auto_discovered=False,
                ),
                installed_by_id.get(bundle.id),
            )
            continue
        if not _SKILL_ID_RE.fullmatch(skill_id):
            continue
        inventory_by_id[skill_id] = _merge_installed_metadata(
            _skill_record_from_registry(workspace_dir, raw),
            installed_by_id.get(skill_id),
        )

    for bundle_dir in discover_skill_bundle_dirs(workspace_dir):
        bundle = load_skill_bundle(bundle_dir, workspace_dir=workspace_dir)
        if bundle is None or bundle.id in inventory_by_id:
            continue
        inventory_by_id[bundle.id] = _merge_installed_metadata(
            _skill_record_from_bundle(
                workspace_dir,
                bundle,
                source="bundle",
                auto_discovered=True,
            ),
            installed_by_id.get(bundle.id),
        )

    return sorted(inventory_by_id.values(), key=lambda item: str(item.get("id", "")))


def _load_registry_bundle(workspace_dir: Path, raw: dict[str, Any]) -> SkillBundle | None:
    resolved = _resolve_registered_skill_path(workspace_dir, raw)
    if resolved is None:
        return None
    return load_skill_bundle(resolved, workspace_dir=workspace_dir, registry_entry=raw)


def _skill_record_from_bundle(
    workspace_dir: Path,
    bundle: SkillBundle,
    *,
    source: str,
    auto_discovered: bool,
) -> dict[str, Any]:
    return {
        "id": bundle.id,
        "name": bundle.name,
        "description": bundle.description,
        "path": bundle.skill_file.relative_to(workspace_dir).as_posix(),
        "scope": bundle.scope,
        "enabled": bundle.enabled,
        "exists": True,
        "source": source,
        "auto_discovered": auto_discovered,
        "bundle_dir": str(bundle.bundle_dir),
        "scripts_dir": str(bundle.scripts_dir) if bundle.scripts_dir else "",
        "references_dir": str(bundle.references_dir) if bundle.references_dir else "",
        "assets_dir": str(bundle.assets_dir) if bundle.assets_dir else "",
        "registry_path": bundle.registry_path or "",
        "primary_env": bundle.metadata.primary_env or "",
        "homepage": bundle.metadata.homepage or "",
        "requires_bins": list(bundle.metadata.requires.bins),
        "requires_env": list(bundle.metadata.requires.env),
        "requires_config": list(bundle.metadata.requires.config),
    }


def _skill_record_from_registry(workspace_dir: Path, raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw.get("id", "")).strip(),
        "name": str(raw.get("name", "")).strip(),
        "description": str(raw.get("description", "")).strip(),
        "path": str(raw.get("path", "")).strip(),
        "scope": _resolve_scope_value(raw.get("scope")),
        "enabled": bool(raw.get("enabled", True)),
        "exists": _skill_path_exists(workspace_dir, raw),
        "source": "registry",
        "auto_discovered": False,
        "bundle_dir": "",
        "scripts_dir": "",
        "references_dir": "",
        "assets_dir": "",
        "registry_path": str(raw.get("path", "")).strip(),
        "primary_env": "",
        "homepage": "",
        "requires_bins": [],
        "requires_env": [],
        "requires_config": [],
        "installer_managed": False,
        "trusted": True,
        "has_scripts": False,
    }


def _resolve_scope_value(value: Any) -> str:
    scope = str(value or "both").strip().lower() or "both"
    return scope if scope in {"queen", "worker", "both"} else "both"


def _evaluate_skill_status(skill_data: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(skill_data.get("enabled", True))
    exists = bool(skill_data.get("exists", False))
    missing_bins = _missing_binaries(skill_data.get("requires_bins"))
    missing_env = _missing_env_vars(skill_data.get("requires_env"))
    missing_config = _missing_config_requirements(skill_data.get("requires_config"))

    reasons: list[str] = []
    if not exists:
        reasons.append("missing SKILL.md")
    if missing_bins:
        reasons.append("missing binaries: " + ", ".join(missing_bins))
    if missing_env:
        reasons.append("missing env: " + ", ".join(missing_env))
    if missing_config:
        reasons.append("missing config: " + ", ".join(missing_config))
    if bool(skill_data.get("has_scripts", False)) and bool(skill_data.get("installer_managed", False)) and not bool(skill_data.get("trusted", True)):
        reasons.append("skill scripts are not trusted yet")

    if not enabled:
        status = "disabled"
        ready = False
    elif reasons:
        status = "not_ready"
        ready = False
    else:
        status = "ready"
        ready = True

    return {
        "status": status,
        "ready": ready,
        "missing_bins": missing_bins,
        "missing_env": missing_env,
        "missing_config": missing_config,
        "reasons": reasons,
    }


def _merge_installed_metadata(skill_data: dict[str, Any], installed_record: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(installed_record, dict):
        skill_data.setdefault("installer_managed", False)
        skill_data.setdefault("trusted", True)
        skill_data.setdefault("has_scripts", bool(skill_data.get("scripts_dir")))
        return skill_data
    merged = dict(skill_data)
    merged["installer_managed"] = True
    merged["trusted"] = bool(installed_record.get("trusted", False))
    merged["has_scripts"] = bool(installed_record.get("has_scripts", bool(skill_data.get("scripts_dir"))))
    merged["installed_source"] = str(installed_record.get("source", "")).strip()
    merged["installed_source_kind"] = str(installed_record.get("source_kind", "")).strip()
    return merged


def _load_installed_manifest(workspace_dir: Path) -> dict[str, Any]:
    path = workspace_dir / "skills" / "installed.json"
    if not path.exists():
        return {"version": 1, "installs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "installs": []}
    if not isinstance(payload, dict):
        return {"version": 1, "installs": []}
    installs = payload.get("installs")
    if not isinstance(installs, list):
        payload["installs"] = []
    return payload


def _missing_binaries(value: Any) -> list[str]:
    missing: list[str] = []
    for item in _normalize_string_list(value):
        if shutil.which(item):
            continue
        missing.append(item)
    return missing


def _missing_env_vars(value: Any) -> list[str]:
    missing: list[str] = []
    for item in _normalize_string_list(value):
        if os.getenv(item):
            continue
        missing.append(item)
    return missing


def _missing_config_requirements(value: Any) -> list[str]:
    missing: list[str] = []
    for item in _normalize_string_list(value):
        env_key = f"BROODMIND_SKILL_CONFIG_{item.upper()}"
        if os.getenv(env_key):
            continue
        missing.append(item)
    return missing


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _resolve_skill_script_path(scripts_dir: Path, script_raw: str) -> Path:
    candidate = (scripts_dir / script_raw).resolve()
    try:
        candidate.relative_to(scripts_dir)
    except ValueError as exc:
        raise ValueError("script must stay inside the skill scripts directory.") from exc
    if not candidate.exists() or not candidate.is_file():
        raise ValueError("script file not found.")
    return candidate


def _resolve_skill_workdir(workspace_dir: Path, ctx: dict[str, Any], args: dict[str, Any]) -> Path | str:
    base_dir = ctx.get("base_dir")
    candidate_base = base_dir if isinstance(base_dir, Path) else workspace_dir
    base_path = Path(candidate_base).resolve()

    workdir_raw = str(args.get("workdir", "")).strip()
    if not workdir_raw:
        try:
            base_path.relative_to(workspace_dir)
        except ValueError:
            return workspace_dir
        return base_path

    try:
        return resolve_workspace_path(base_path, workdir_raw, must_exist=True)
    except WorkspacePathError as exc:
        return f"run_skill_script error: invalid workdir: {exc}"


def _normalize_script_args(value: Any) -> list[str] | None:
    if value is None:
        return []
    if not isinstance(value, list):
        return None
    return [str(item) for item in value]


def _build_skill_script_command(script_path: Path, runner: str) -> list[str]:
    explicit_runner = runner.strip().lower()
    suffix = script_path.suffix.lower()

    if explicit_runner == "direct":
        return [str(script_path)]

    if explicit_runner in {"python", "python3"}:
        executable = sys.executable if explicit_runner == "python" else shutil.which(explicit_runner)
        if not executable:
            raise ValueError(f"runner '{explicit_runner}' is not available.")
        return [executable, str(script_path)]

    if explicit_runner in {"node", "bash", "sh", "powershell", "pwsh"}:
        executable = shutil.which(explicit_runner)
        if not executable:
            raise ValueError(f"runner '{explicit_runner}' is not available.")
        if explicit_runner in {"powershell", "pwsh"}:
            return [executable, "-NoProfile", "-File", str(script_path)]
        return [executable, str(script_path)]

    if explicit_runner:
        raise ValueError(f"unsupported runner '{explicit_runner}'.")

    if suffix == ".py":
        return [sys.executable, str(script_path)]
    if suffix == ".js":
        executable = shutil.which("node")
        if not executable:
            raise ValueError("runner 'node' is not available.")
        return [executable, str(script_path)]
    if suffix == ".ps1":
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if not executable:
            raise ValueError("runner 'pwsh' or 'powershell' is not available.")
        return [executable, "-NoProfile", "-File", str(script_path)]
    if suffix == ".sh":
        executable = shutil.which("bash") or shutil.which("sh")
        if not executable:
            raise ValueError("runner 'bash' or 'sh' is not available.")
        return [executable, str(script_path)]

    raise ValueError("runner is required for this script type.")


def _slugify(value: str) -> str:
    lowered = value.lower().strip()
    lowered = re.sub(r"[^a-z0-9_-]+", "_", lowered)
    lowered = re.sub(r"_+", "_", lowered).strip("_")
    return lowered


def _bounded_int(value: Any, *, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(low, min(high, parsed))
