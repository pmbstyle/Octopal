from __future__ import annotations

import json
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from broodmind.tools.skills.bundles import SkillBundle, load_skill_bundle
from broodmind.tools.skills.management import ensure_skills_layout
from broodmind.tools.skills.runtime_envs import build_runtime_install_hint
from broodmind.tools.skills.scanner import scan_skill_bundle

_DEFAULT_CLAWHUB_SITE = "https://clawhub.ai"
_INSTALL_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class SkillInstallSource:
    kind: str
    value: str
    normalized: str


def detect_skill_install_source(source: str) -> SkillInstallSource:
    raw = str(source or "").strip()
    if not raw:
        raise ValueError("source is required")

    if raw.startswith("clawhub:"):
        slug = raw.split(":", 1)[1].strip().strip("/")
        if not slug:
            raise ValueError("clawhub slug is required")
        return SkillInstallSource(kind="clawhub_slug", value=slug, normalized=slug)

    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"}:
        normalized = raw
        lowered_path = parsed.path.lower()
        if lowered_path.endswith(".zip"):
            return SkillInstallSource(kind="zip_url", value=raw, normalized=normalized)
        if lowered_path.endswith("/skill.md") or lowered_path.endswith("/skill.md/") or lowered_path.endswith("skill.md"):
            return SkillInstallSource(kind="skill_md_url", value=raw, normalized=normalized)
        return SkillInstallSource(kind="remote_url", value=raw, normalized=normalized)

    candidate = Path(raw)
    if candidate.exists():
        resolved = candidate.resolve()
        if resolved.is_dir():
            return SkillInstallSource(kind="local_dir", value=str(resolved), normalized=str(resolved))
        if resolved.is_file() and resolved.name.lower() == "skill.md":
            return SkillInstallSource(kind="local_skill_md", value=str(resolved), normalized=str(resolved))
        if resolved.is_file() and resolved.suffix.lower() == ".zip":
            return SkillInstallSource(kind="local_zip", value=str(resolved), normalized=str(resolved))
        raise ValueError("local source must be a skill directory, SKILL.md, or .zip archive")

    return SkillInstallSource(kind="clawhub_slug", value=raw.strip("/"), normalized=raw.strip("/"))


def install_skill_from_source(
    source: str,
    *,
    workspace_dir: Path,
    clawhub_site: str = _DEFAULT_CLAWHUB_SITE,
    trusted: bool | None = None,
) -> dict[str, Any]:
    ensure_skills_layout(workspace_dir)
    install_source = detect_skill_install_source(source)
    with tempfile.TemporaryDirectory(prefix="broodmind-skill-install-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        bundle_root = _materialize_install_source(
            install_source,
            scratch_dir=temp_dir,
            clawhub_site=clawhub_site,
        )
        bundle = load_skill_bundle(bundle_root)
        if bundle is None:
            raise ValueError("downloaded source does not contain a valid skill bundle")
        destination = _prepare_install_destination(workspace_dir, bundle, install_source)
        _copy_bundle(bundle.bundle_dir, destination)
        manifest = _read_install_manifest(workspace_dir)
        record = _build_install_record(
            bundle,
            install_source,
            clawhub_site,
            destination,
            trusted=trusted,
        )
        installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
        installs = [item for item in installs if str(item.get("skill_id", "")) != bundle.id]
        installs.append(record)
        manifest["installs"] = sorted(installs, key=lambda item: str(item.get("skill_id", "")))
        _write_install_manifest(workspace_dir, manifest)
        next_step = build_runtime_install_hint(bundle.id, workspace_dir=workspace_dir)
        return {
            "status": "installed",
            "skill_id": bundle.id,
            "name": bundle.name,
            "description": bundle.description,
            "path": str(destination / bundle.skill_file.name),
            "source_kind": install_source.kind,
            "source": install_source.normalized,
            "trusted": bool(record.get("trusted", False)),
            "has_scripts": bool(record.get("has_scripts", False)),
            "next_step": next_step,
            "manifest_path": str(_install_manifest_path(workspace_dir)),
        }


def list_installed_skill_sources(workspace_dir: Path) -> dict[str, Any]:
    manifest = _read_install_manifest(workspace_dir)
    installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
    return {
        "count": len(installs),
        "manifest_path": str(_install_manifest_path(workspace_dir)),
        "installs": installs,
    }


def update_installed_skill(
    skill_id: str,
    *,
    workspace_dir: Path,
    clawhub_site: str | None = None,
) -> dict[str, Any]:
    record = get_installed_skill_record(workspace_dir, skill_id)
    if record is None:
        raise ValueError(f"skill '{skill_id}' is not installer-managed")
    source = str(record.get("source", "")).strip()
    if not source:
        raise ValueError(f"skill '{skill_id}' does not have a stored source")
    resolved_site = clawhub_site or str(record.get("clawhub_site", "")).strip() or _DEFAULT_CLAWHUB_SITE
    trusted = bool(record.get("trusted", False))
    payload = install_skill_from_source(
        source,
        workspace_dir=workspace_dir,
        clawhub_site=resolved_site,
        trusted=trusted,
    )
    payload["status"] = "updated"
    payload["previous_source"] = source
    return payload


def remove_installed_skill(skill_id: str, *, workspace_dir: Path) -> dict[str, Any]:
    manifest = _read_install_manifest(workspace_dir)
    installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
    record = next((item for item in installs if str(item.get("skill_id", "")) == skill_id), None)
    if record is None:
        raise ValueError(f"skill '{skill_id}' is not installer-managed")

    removed_path = False
    bundle_path_raw = str(record.get("path", "")).strip()
    if bundle_path_raw:
        bundle_path = Path(bundle_path_raw).resolve()
        try:
            bundle_path.relative_to((workspace_dir / "skills").resolve())
        except ValueError as exc:
            raise ValueError("stored install path points outside workspace skills directory") from exc
        bundle_dir = bundle_path.parent
        if bundle_dir.exists():
            shutil.rmtree(bundle_dir)
            removed_path = True

    installs = [item for item in installs if str(item.get("skill_id", "")) != skill_id]
    manifest["installs"] = installs
    _write_install_manifest(workspace_dir, manifest)
    return {
        "status": "removed",
        "skill_id": skill_id,
        "removed_path": removed_path,
        "manifest_path": str(_install_manifest_path(workspace_dir)),
    }


def get_installed_skill_record(workspace_dir: Path, skill_id: str) -> dict[str, Any] | None:
    manifest = _read_install_manifest(workspace_dir)
    installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
    return next((item for item in installs if str(item.get("skill_id", "")) == skill_id), None)


def set_installed_skill_trust(
    skill_id: str,
    *,
    workspace_dir: Path,
    trusted: bool,
    force: bool = False,
) -> dict[str, Any]:
    manifest = _read_install_manifest(workspace_dir)
    installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
    updated = False
    for item in installs:
        if str(item.get("skill_id", "")) != skill_id:
            continue
        scan = item.get("script_scan")
        scan_status = str(scan.get("status", "")).strip() if isinstance(scan, dict) else ""
        if trusted and bool(item.get("has_scripts", False)):
            if scan_status == "review_required" and not force:
                raise ValueError(
                    f"skill '{skill_id}' has script scan findings; run `broodmind skill verify {skill_id}` and re-run trust with --force after review"
                )
            if not scan_status:
                raise ValueError(
                    f"skill '{skill_id}' has not been verified yet; run `broodmind skill verify {skill_id}` first"
                )
        item["trusted"] = bool(trusted)
        updated = True
        break
    if not updated:
        raise ValueError(f"skill '{skill_id}' is not installer-managed")
    manifest["installs"] = installs
    _write_install_manifest(workspace_dir, manifest)
    return {
        "status": "trusted" if trusted else "untrusted",
        "skill_id": skill_id,
        "trusted": bool(trusted),
        "manifest_path": str(_install_manifest_path(workspace_dir)),
    }


def verify_installed_skill(skill_id: str, *, workspace_dir: Path) -> dict[str, Any]:
    manifest = _read_install_manifest(workspace_dir)
    installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
    updated_record: dict[str, Any] | None = None
    for item in installs:
        if str(item.get("skill_id", "")) != skill_id:
            continue
        skill_path_raw = str(item.get("path", "")).strip()
        if not skill_path_raw:
            raise ValueError(f"skill '{skill_id}' does not have a stored path")
        skill_path = Path(skill_path_raw).resolve()
        if not skill_path.exists():
            raise ValueError(f"installed bundle for '{skill_id}' is missing")
        bundle = load_skill_bundle(skill_path.parent)
        if bundle is None:
            raise ValueError(f"installed bundle for '{skill_id}' is invalid")
        scan = scan_skill_bundle(bundle)
        item["has_scripts"] = bool(bundle.scripts_dir)
        item["script_scan"] = scan
        updated_record = item
        break
    if updated_record is None:
        raise ValueError(f"skill '{skill_id}' is not installer-managed")
    manifest["installs"] = installs
    _write_install_manifest(workspace_dir, manifest)
    return {
        "status": "verified",
        "skill_id": skill_id,
        "trusted": bool(updated_record.get("trusted", False)),
        "has_scripts": bool(updated_record.get("has_scripts", False)),
        "script_scan": updated_record.get("script_scan", {}),
        "manifest_path": str(_install_manifest_path(workspace_dir)),
    }


def _materialize_install_source(
    install_source: SkillInstallSource,
    *,
    scratch_dir: Path,
    clawhub_site: str,
) -> Path:
    if install_source.kind == "clawhub_slug":
        archive_path = scratch_dir / "skill.zip"
        _download_clawhub_archive(install_source.value, archive_path=archive_path, clawhub_site=clawhub_site)
        extracted_dir = scratch_dir / "bundle"
        extracted_dir.mkdir(parents=True, exist_ok=True)
        _extract_archive(archive_path, extracted_dir)
        return _discover_bundle_root(extracted_dir)

    if install_source.kind in {"zip_url", "remote_url"}:
        archive_path = scratch_dir / "skill.zip"
        _download_to_path(install_source.value, archive_path)
        extracted_dir = scratch_dir / "bundle"
        extracted_dir.mkdir(parents=True, exist_ok=True)
        _extract_archive(archive_path, extracted_dir)
        return _discover_bundle_root(extracted_dir)

    if install_source.kind == "skill_md_url":
        bundle_dir = scratch_dir / _infer_bundle_name_from_url(install_source.value)
        bundle_dir.mkdir(parents=True, exist_ok=True)
        _download_to_path(install_source.value, bundle_dir / "SKILL.md")
        return bundle_dir

    if install_source.kind == "local_dir":
        return Path(install_source.value)

    if install_source.kind == "local_skill_md":
        return Path(install_source.value).parent

    if install_source.kind == "local_zip":
        extracted_dir = scratch_dir / "bundle"
        extracted_dir.mkdir(parents=True, exist_ok=True)
        _extract_archive(Path(install_source.value), extracted_dir)
        return _discover_bundle_root(extracted_dir)

    raise ValueError(f"unsupported install source kind: {install_source.kind}")


def _download_clawhub_archive(slug: str, *, archive_path: Path, clawhub_site: str) -> None:
    base = clawhub_site.rstrip("/")
    candidates = [
        (f"{base}/api/v1/download", {"slug": slug}),
        (f"{base}/api/v1/download/{slug}", None),
    ]
    errors: list[str] = []
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for url, params in candidates:
            try:
                response = client.get(url, params=params)
                if response.status_code >= 400:
                    errors.append(f"{url} -> HTTP {response.status_code}")
                    continue
                archive_path.write_bytes(response.content)
                return
            except Exception as exc:
                errors.append(f"{url} -> {exc}")
    raise ValueError("failed to download skill from ClawHub: " + "; ".join(errors))


def _download_to_path(url: str, target_path: Path) -> None:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        target_path.write_bytes(response.content)


def _extract_archive(archive_path: Path, destination: Path) -> None:
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            archive.extractall(destination)
    except Exception as exc:
        raise ValueError(f"failed to extract skill archive: {exc}") from exc


def _discover_bundle_root(root: Path) -> Path:
    for candidate_name in ("SKILL.md", "skill.md"):
        direct_skill = root / candidate_name
        if direct_skill.exists():
            return root

    candidates: list[Path] = []
    for skill_file in root.rglob("*"):
        if not skill_file.is_file():
            continue
        if skill_file.name.lower() != "skill.md":
            continue
        candidates.append(skill_file.parent)
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise ValueError("archive does not contain SKILL.md")
    raise ValueError("archive contains multiple skill bundles; choose a narrower source")


def _infer_bundle_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    parent_name = Path(parsed.path).parent.name.strip()
    if parent_name:
        return parent_name
    return "imported_skill"


def _prepare_install_destination(
    workspace_dir: Path,
    bundle: SkillBundle,
    install_source: SkillInstallSource,
) -> Path:
    destination = workspace_dir / "skills" / bundle.id
    if not destination.exists():
        return destination

    manifest = _read_install_manifest(workspace_dir)
    installs = [item for item in manifest.get("installs", []) if isinstance(item, dict)]
    existing = next((item for item in installs if str(item.get("skill_id", "")) == bundle.id), None)
    if existing is None:
        raise ValueError(
            f"skill '{bundle.id}' already exists locally; refusing to overwrite unmanaged bundle"
        )
    existing_source = str(existing.get("source", "")).strip()
    if existing_source != install_source.normalized:
        raise ValueError(
            f"skill '{bundle.id}' is already installed from a different source: {existing_source}"
        )
    shutil.rmtree(destination)
    return destination


def _copy_bundle(source_dir: Path, destination: Path) -> None:
    shutil.copytree(source_dir, destination, dirs_exist_ok=False)


def _build_install_record(
    bundle: SkillBundle,
    install_source: SkillInstallSource,
    clawhub_site: str,
    destination: Path,
    *,
    trusted: bool | None,
) -> dict[str, Any]:
    resolved_trust = _default_trust_for_source(install_source) if trusted is None else bool(trusted)
    script_scan = scan_skill_bundle(bundle)
    return {
        "skill_id": bundle.id,
        "name": bundle.name,
        "description": bundle.description,
        "path": str(destination / bundle.skill_file.name),
        "source_kind": install_source.kind,
        "source": install_source.normalized,
        "clawhub_site": clawhub_site.rstrip("/") if install_source.kind == "clawhub_slug" else "",
        "trusted": resolved_trust,
        "has_scripts": bool(bundle.scripts_dir),
        "script_scan": script_scan,
    }


def _install_manifest_path(workspace_dir: Path) -> Path:
    return workspace_dir / "skills" / "installed.json"


def _read_install_manifest(workspace_dir: Path) -> dict[str, Any]:
    path = _install_manifest_path(workspace_dir)
    if not path.exists():
        return {"version": _INSTALL_MANIFEST_VERSION, "installs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": _INSTALL_MANIFEST_VERSION, "installs": []}
    if not isinstance(payload, dict):
        return {"version": _INSTALL_MANIFEST_VERSION, "installs": []}
    installs = payload.get("installs")
    if not isinstance(installs, list):
        payload["installs"] = []
    payload.setdefault("version", _INSTALL_MANIFEST_VERSION)
    return payload


def _write_install_manifest(workspace_dir: Path, payload: dict[str, Any]) -> None:
    path = _install_manifest_path(workspace_dir)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")


def _default_trust_for_source(install_source: SkillInstallSource) -> bool:
    return install_source.kind in {"local_dir", "local_skill_md", "local_zip"}
