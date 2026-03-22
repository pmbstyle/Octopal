from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from broodmind.tools.skills.bundles import SkillBundle, discover_skill_bundle_dirs, load_skill_bundle

_PYTHON_SCRIPT_SUFFIXES = {".py"}
_NODE_SCRIPT_SUFFIXES = {".js", ".mjs", ".cjs", ".ts"}


def detect_skill_runtime(bundle: SkillBundle) -> dict[str, Any]:
    script_suffixes = _collect_script_suffixes(bundle.scripts_dir)
    has_python_scripts = any(suffix in _PYTHON_SCRIPT_SUFFIXES for suffix in script_suffixes)
    has_node_scripts = any(suffix in _NODE_SCRIPT_SUFFIXES for suffix in script_suffixes)
    python_packages = list(bundle.metadata.runtime.python.packages)
    node_packages = list(bundle.metadata.runtime.node.packages)
    package_manager = bundle.metadata.runtime.node.package_manager

    if not python_packages:
        python_packages = _read_python_requirements(bundle.bundle_dir)
    if not node_packages:
        node_packages, package_manager = _read_node_package_manifest(bundle.bundle_dir, package_manager)

    if (has_python_scripts or python_packages) and (has_node_scripts or node_packages):
        return {
            "kind": "mixed",
            "required": True,
            "recommended": True,
            "reason": "mixed python and node runtimes are not supported yet",
            "python_packages": python_packages,
            "node_packages": node_packages,
            "package_manager": package_manager,
        }

    if has_python_scripts or python_packages:
        return {
            "kind": "python",
            "required": True,
            "recommended": has_python_scripts or bool(python_packages),
            "reason": "",
            "python_packages": python_packages,
            "node_packages": [],
            "package_manager": "",
        }

    if has_node_scripts or node_packages:
        return {
            "kind": "node",
            "required": True,
            "recommended": has_node_scripts or bool(node_packages),
            "reason": "",
            "python_packages": [],
            "node_packages": node_packages,
            "package_manager": package_manager,
        }

    return {
        "kind": "",
        "required": False,
        "recommended": False,
        "reason": "",
        "python_packages": [],
        "node_packages": [],
        "package_manager": "",
    }


def get_skill_env_status(skill_id: str, *, workspace_dir: Path) -> dict[str, Any]:
    bundle = _load_workspace_skill_bundle(skill_id, workspace_dir)
    if bundle is None:
        return {
            "skill_id": skill_id,
            "kind": "",
            "required": False,
            "recommended": False,
            "prepared": False,
            "status": "missing_skill",
            "reason": f"skill '{skill_id}' not found",
            "manifest_path": "",
            "next_step": "",
        }

    runtime = detect_skill_runtime(bundle)
    env_dir = _skill_env_dir(workspace_dir, skill_id)
    manifest_path = _skill_env_manifest_path(workspace_dir, skill_id)
    manifest = _read_env_manifest(workspace_dir, skill_id)

    if runtime["kind"] == "":
        return {
            "skill_id": skill_id,
            **runtime,
            "prepared": False,
            "status": "not_applicable",
            "manifest_path": str(manifest_path),
            "next_step": "",
        }

    if runtime["kind"] == "mixed":
        return {
            "skill_id": skill_id,
            **runtime,
            "prepared": False,
            "status": "unsupported",
            "manifest_path": str(manifest_path),
            "next_step": "",
        }

    prepared = bool(manifest) and _is_env_manifest_usable(manifest, env_dir)
    status = "prepared" if prepared else "missing"
    next_step = f"uv run broodmind skill prepare-env {skill_id}" if runtime["recommended"] else ""
    reason = runtime["reason"]
    if runtime["required"] and not prepared:
        reason = f"runtime env is not prepared; run `{next_step}`"
    return {
        "skill_id": skill_id,
        **runtime,
        "prepared": prepared,
        "status": status,
        "reason": reason,
        "manifest_path": str(manifest_path),
        "next_step": next_step,
    }


def prepare_skill_env(skill_id: str, *, workspace_dir: Path) -> dict[str, Any]:
    bundle = _load_workspace_skill_bundle(skill_id, workspace_dir)
    if bundle is None:
        raise ValueError(f"skill '{skill_id}' not found")

    runtime = detect_skill_runtime(bundle)
    if runtime["kind"] == "":
        return {
            "status": "not_applicable",
            "skill_id": skill_id,
            "kind": "",
            "message": "skill does not require an isolated runtime env",
        }
    if runtime["kind"] == "mixed":
        raise ValueError("mixed python and node runtimes are not supported yet")

    env_root = _skill_env_root(workspace_dir)
    env_dir = _skill_env_dir(workspace_dir, skill_id)
    staging_dir = Path(tempfile.mkdtemp(prefix=f"{skill_id}-staging-", dir=str(env_root)))

    try:
        if runtime["kind"] == "python":
            _prepare_python_env(staging_dir, runtime["python_packages"])
            executable = _python_env_executable(staging_dir)
        else:
            _prepare_node_env(staging_dir, runtime["node_packages"])
            executable = _node_env_runner(staging_dir)

        manifest = {
            "version": 1,
            "skill_id": skill_id,
            "kind": runtime["kind"],
            "created_at": datetime.now(UTC).isoformat(),
            "python_packages": runtime["python_packages"],
            "node_packages": runtime["node_packages"],
            "package_manager": runtime["package_manager"],
            "executable": str(executable),
        }
        _write_env_manifest_at(staging_dir / "env.json", manifest)
        _replace_skill_env_dir(env_dir, staging_dir)
    except Exception:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return {
        "status": "prepared",
        "skill_id": skill_id,
        "kind": runtime["kind"],
        "env_dir": str(env_dir),
        "manifest_path": str(_skill_env_manifest_path(workspace_dir, skill_id)),
        "executable": str(executable),
        "python_packages": runtime["python_packages"],
        "node_packages": runtime["node_packages"],
    }


def remove_skill_env(skill_id: str, *, workspace_dir: Path) -> dict[str, Any]:
    env_dir = _skill_env_dir(workspace_dir, skill_id)
    removed = False
    if env_dir.exists():
        shutil.rmtree(env_dir)
        removed = True
    return {
        "status": "removed",
        "skill_id": skill_id,
        "removed": removed,
        "env_dir": str(env_dir),
    }


def build_runtime_install_hint(skill_id: str, *, workspace_dir: Path) -> str:
    status = get_skill_env_status(skill_id, workspace_dir=workspace_dir)
    if not status.get("recommended", False):
        return ""
    return str(status.get("next_step", ""))


def resolve_skill_runtime_execution(
    skill_id: str,
    *,
    workspace_dir: Path,
    script_path: Path,
    explicit_runner: str,
) -> dict[str, Any]:
    status = get_skill_env_status(skill_id, workspace_dir=workspace_dir)
    env_manifest = _read_env_manifest(workspace_dir, skill_id)
    suffix = script_path.suffix.lower()

    if status["kind"] == "python" and status["prepared"]:
        return {
            "runner": [str(_python_env_executable(_skill_env_dir(workspace_dir, skill_id))), str(script_path)],
            "env": None,
        }

    if status["kind"] == "node" and status["prepared"]:
        env_dir = _skill_env_dir(workspace_dir, skill_id)
        if suffix == ".ts":
            tsx_binary = _node_env_binary(env_dir, "tsx")
            if not tsx_binary.exists():
                raise ValueError("typescript skill runtime requires the 'tsx' package in the prepared node env.")
            return {
                "runner": [str(tsx_binary), str(script_path)],
                "env": _node_env_process_env(env_dir),
            }
        if suffix in {".js", ".mjs", ".cjs"}:
            node_binary = shutil.which("node")
            if not node_binary:
                raise ValueError("runner 'node' is not available.")
            return {
                "runner": [node_binary, str(script_path)],
                "env": _node_env_process_env(env_dir),
            }

    if status["required"] and not status["prepared"]:
        next_step = status.get("next_step", "")
        raise ValueError(f"skill runtime env is not prepared. Next step: {next_step}")

    return {
        "runner": [],
        "env": None,
    }


def _load_workspace_skill_bundle(skill_id: str, workspace_dir: Path) -> SkillBundle | None:
    skill_dir = workspace_dir / "skills" / skill_id
    if not skill_dir.exists():
        skill_dir = None
    if skill_dir is not None:
        bundle = load_skill_bundle(skill_dir, workspace_dir=workspace_dir)
        if bundle is not None and bundle.id == skill_id:
            return bundle

    registry = _read_skills_registry(workspace_dir)
    for item in registry.get("skills", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("id", "")).strip() != skill_id:
            continue
        resolved = _resolve_registry_skill_path(workspace_dir, str(item.get("path", "")).strip())
        if resolved is None:
            continue
        bundle = load_skill_bundle(resolved, workspace_dir=workspace_dir, registry_entry=item)
        if bundle is not None and bundle.id == skill_id:
            return bundle

    for bundle_dir in discover_skill_bundle_dirs(workspace_dir):
        bundle = load_skill_bundle(bundle_dir, workspace_dir=workspace_dir)
        if bundle is not None and bundle.id == skill_id:
            return bundle
    return None


def _skill_env_root(workspace_dir: Path) -> Path:
    root = workspace_dir / ".skill-envs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _skill_env_dir(workspace_dir: Path, skill_id: str) -> Path:
    return _skill_env_root(workspace_dir) / skill_id


def _skill_env_manifest_path(workspace_dir: Path, skill_id: str) -> Path:
    return _skill_env_dir(workspace_dir, skill_id) / "env.json"


def _read_env_manifest(workspace_dir: Path, skill_id: str) -> dict[str, Any]:
    path = _skill_env_manifest_path(workspace_dir, skill_id)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_env_manifest(workspace_dir: Path, skill_id: str, payload: dict[str, Any]) -> None:
    path = _skill_env_manifest_path(workspace_dir, skill_id)
    _write_env_manifest_at(path, payload)


def _write_env_manifest_at(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _collect_script_suffixes(scripts_dir: Path | None) -> set[str]:
    if scripts_dir is None or not scripts_dir.exists():
        return set()
    return {path.suffix.lower() for path in scripts_dir.rglob("*") if path.is_file() and path.suffix}


def _read_python_requirements(bundle_dir: Path) -> list[str]:
    requirements_path = bundle_dir / "requirements.txt"
    if not requirements_path.exists() or not requirements_path.is_file():
        return []
    packages: list[str] = []
    for raw_line in requirements_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("-r", "--requirement")):
            continue
        packages.append(line)
    return packages


def _read_node_package_manifest(bundle_dir: Path, fallback_package_manager: str) -> tuple[list[str], str]:
    package_json_path = bundle_dir / "package.json"
    if not package_json_path.exists() or not package_json_path.is_file():
        return [], fallback_package_manager
    try:
        payload = json.loads(package_json_path.read_text(encoding="utf-8"))
    except Exception:
        return [], fallback_package_manager
    if not isinstance(payload, dict):
        return [], fallback_package_manager

    packages: list[str] = []
    for key in ("dependencies", "devDependencies", "optionalDependencies"):
        block = payload.get(key)
        if not isinstance(block, dict):
            continue
        for package_name, package_version in block.items():
            name = str(package_name).strip()
            version = str(package_version).strip()
            if not name:
                continue
            packages.append(f"{name}@{version}" if version else name)

    package_manager = _normalize_node_package_manager(payload.get("packageManager"), fallback_package_manager)
    return packages, package_manager


def _normalize_node_package_manager(value: Any, fallback: str) -> str:
    raw = str(value or fallback or "npm").strip().lower()
    if raw.startswith("npm@"):
        return "npm"
    return raw if raw in {"npm"} else "npm"


def _prepare_python_env(env_dir: Path, packages: list[str]) -> None:
    subprocess.run([sys.executable, "-m", "venv", str(env_dir)], check=True, capture_output=True, text=True)
    if not packages:
        return
    python_executable = _python_env_executable(env_dir)
    subprocess.run(
        [str(python_executable), "-m", "pip", "install", *packages],
        check=True,
        capture_output=True,
        text=True,
    )


def _prepare_node_env(env_dir: Path, packages: list[str]) -> None:
    npm_binary = shutil.which("npm")
    if not npm_binary:
        raise ValueError("npm is required to prepare a node skill env.")
    package_json = {
        "name": env_dir.name,
        "private": True,
        "version": "0.0.0",
    }
    (env_dir / "package.json").write_text(json.dumps(package_json, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if packages:
        subprocess.run(
            [npm_binary, "install", "--no-save", *packages],
            cwd=str(env_dir),
            check=True,
            capture_output=True,
            text=True,
        )


def _python_env_executable(env_dir: Path) -> Path:
    if os.name == "nt":
        return env_dir / "Scripts" / "python.exe"
    return env_dir / "bin" / "python"


def _node_env_binary(env_dir: Path, name: str) -> Path:
    if os.name == "nt":
        return env_dir / "node_modules" / ".bin" / f"{name}.cmd"
    return env_dir / "node_modules" / ".bin" / name


def _node_env_runner(env_dir: Path) -> Path:
    node_binary = shutil.which("node")
    if node_binary:
        return Path(node_binary)
    return _node_env_binary(env_dir, "node")


def _node_env_process_env(env_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    node_modules = env_dir / "node_modules"
    existing_node_path = env.get("NODE_PATH", "")
    env["NODE_PATH"] = str(node_modules) if not existing_node_path else os.pathsep.join([str(node_modules), existing_node_path])
    return env


def _is_env_manifest_usable(manifest: dict[str, Any], env_dir: Path) -> bool:
    kind = str(manifest.get("kind", "")).strip()
    if kind == "python":
        return _python_env_executable(env_dir).exists()
    if kind == "node":
        return (env_dir / "package.json").exists()
    return False


def _replace_skill_env_dir(target_dir: Path, staging_dir: Path) -> None:
    backup_dir: Path | None = None
    try:
        if target_dir.exists():
            backup_dir = target_dir.with_name(f"{target_dir.name}.backup")
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)
            target_dir.replace(backup_dir)
        staging_dir.replace(target_dir)
        if backup_dir and backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)
    except Exception:
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        if backup_dir and backup_dir.exists():
            backup_dir.replace(target_dir)
        raise


def _read_skills_registry(workspace_dir: Path) -> dict[str, Any]:
    path = workspace_dir / "skills" / "registry.json"
    if not path.exists():
        return {"version": 1, "skills": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "skills": []}
    if not isinstance(payload, dict):
        return {"version": 1, "skills": []}
    skills = payload.get("skills")
    if not isinstance(skills, list):
        payload["skills"] = []
    return payload


def _resolve_registry_skill_path(workspace_dir: Path, path_raw: str) -> Path | None:
    if not path_raw:
        return None
    candidate = Path(path_raw)
    if not candidate.is_absolute():
        candidate = workspace_dir / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(workspace_dir.resolve())
    except ValueError:
        return None
    if candidate.is_dir():
        upper = candidate / "SKILL.md"
        lower = candidate / "skill.md"
        if upper.exists():
            return upper
        if lower.exists():
            return lower
        return None
    if candidate.is_file() and candidate.name.lower() == "skill.md":
        return candidate
    return None
