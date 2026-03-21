from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from broodmind.tools.skills.bundles import SkillBundle

_SCRIPT_SUFFIXES = {".py", ".js", ".mjs", ".cjs", ".sh", ".bash", ".ps1", ".cmd", ".bat"}
_RULES: tuple[tuple[str, str, str, tuple[re.Pattern[str], ...]], ...] = (
    (
        "network_access",
        "medium",
        "Script appears to make outbound network requests.",
        (
            re.compile(r"\bcurl\b", re.IGNORECASE),
            re.compile(r"\bwget\b", re.IGNORECASE),
            re.compile(r"\bInvoke-WebRequest\b", re.IGNORECASE),
            re.compile(r"\bInvoke-RestMethod\b", re.IGNORECASE),
            re.compile(r"\brequests\.", re.IGNORECASE),
            re.compile(r"\bhttpx\.", re.IGNORECASE),
            re.compile(r"\burllib\.request\b", re.IGNORECASE),
            re.compile(r"\bsocket\b", re.IGNORECASE),
        ),
    ),
    (
        "process_execution",
        "medium",
        "Script appears to spawn other processes or shells.",
        (
            re.compile(r"\bsubprocess\b", re.IGNORECASE),
            re.compile(r"\bos\.system\b", re.IGNORECASE),
            re.compile(r"shell\s*=\s*True", re.IGNORECASE),
            re.compile(r"\bStart-Process\b", re.IGNORECASE),
            re.compile(r"\bcmd\.exe\b", re.IGNORECASE),
        ),
    ),
    (
        "destructive_filesystem",
        "high",
        "Script appears to delete or recursively remove files.",
        (
            re.compile(r"\brm\s+-rf\b", re.IGNORECASE),
            re.compile(r"\bshutil\.rmtree\b", re.IGNORECASE),
            re.compile(r"\bRemove-Item\b", re.IGNORECASE),
            re.compile(r"\bdel\s+/f\b", re.IGNORECASE),
        ),
    ),
    (
        "package_install",
        "medium",
        "Script appears to install packages or dependencies at runtime.",
        (
            re.compile(r"\bpip\s+install\b", re.IGNORECASE),
            re.compile(r"\buv\s+pip\s+install\b", re.IGNORECASE),
            re.compile(r"\bnpm\s+install\b", re.IGNORECASE),
            re.compile(r"\byarn\s+add\b", re.IGNORECASE),
            re.compile(r"\bpnpm\s+add\b", re.IGNORECASE),
        ),
    ),
)


def scan_skill_bundle(bundle: SkillBundle) -> dict[str, Any]:
    return scan_skill_bundle_dir(bundle.bundle_dir, bundle.scripts_dir)


def scan_skill_bundle_dir(bundle_dir: Path, scripts_dir: Path | None) -> dict[str, Any]:
    scanned_at = datetime.now(UTC).isoformat()
    if scripts_dir is None or not scripts_dir.exists() or not scripts_dir.is_dir():
        return {
            "status": "no_scripts",
            "scanned_at": scanned_at,
            "file_count": 0,
            "files": [],
            "findings": [],
        }

    files: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    for script_path in sorted(scripts_dir.rglob("*")):
        if not script_path.is_file():
            continue
        relative_path = script_path.relative_to(bundle_dir).as_posix()
        content_bytes = script_path.read_bytes()
        files.append(
            {
                "path": relative_path,
                "sha256": hashlib.sha256(content_bytes).hexdigest(),
                "size": len(content_bytes),
            }
        )

        if script_path.suffix.lower() not in _SCRIPT_SUFFIXES:
            continue

        try:
            content_text = content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            findings.append(
                {
                    "path": relative_path,
                    "rule": "non_utf8_script",
                    "severity": "medium",
                    "message": "Script is not valid UTF-8 and should be reviewed manually.",
                }
            )
            continue

        for rule_name, severity, message, patterns in _RULES:
            if any(pattern.search(content_text) for pattern in patterns):
                findings.append(
                    {
                        "path": relative_path,
                        "rule": rule_name,
                        "severity": severity,
                        "message": message,
                    }
                )

    return {
        "status": "review_required" if findings else "clean",
        "scanned_at": scanned_at,
        "file_count": len(files),
        "files": files,
        "findings": findings,
    }
