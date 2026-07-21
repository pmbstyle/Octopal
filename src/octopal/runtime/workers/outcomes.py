"""Independent, metadata-only outcome checks for worker runs."""

from __future__ import annotations

import hashlib
from pathlib import Path

from octopal.runtime.workers.contracts import (
    WorkerResult,
    WorkerSpec,
    WorkspaceFileVerificationEvidence,
)
from octopal.tools.filesystem.path_safety import WorkspacePathError, resolve_workspace_path


def verify_worker_outcome(
    *,
    spec: WorkerSpec,
    result: WorkerResult,
    worker_status: str,
    workspace_dir: Path,
) -> WorkspaceFileVerificationEvidence | None:
    """Verify an explicitly requested postcondition without trusting worker output."""

    contract = spec.outcome_verification
    if contract is None:
        return None

    path_fingerprint = _fingerprint(contract.artifact_path)
    if worker_status != "completed" or result.status != "completed":
        return WorkspaceFileVerificationEvidence(
            status="not_run",
            artifact_path_fingerprint=path_fingerprint,
            observed_exists=False,
            observed_regular_file=False,
            unresolved_gaps=["worker_did_not_complete"],
        )

    try:
        artifact = resolve_workspace_path(
            workspace_dir,
            contract.artifact_path,
            must_exist=False,
            allowed_paths=spec.allowed_paths,
        )
    except WorkspacePathError:
        return WorkspaceFileVerificationEvidence(
            status="failed",
            artifact_path_fingerprint=path_fingerprint,
            observed_exists=False,
            observed_regular_file=False,
            unresolved_gaps=["artifact_path_rejected"],
        )

    try:
        exists = artifact.exists()
    except OSError:
        return WorkspaceFileVerificationEvidence(
            status="failed",
            artifact_path_fingerprint=path_fingerprint,
            observed_exists=False,
            observed_regular_file=False,
            unresolved_gaps=["artifact_stat_failed"],
        )
    if not exists:
        return WorkspaceFileVerificationEvidence(
            status="failed",
            artifact_path_fingerprint=path_fingerprint,
            observed_exists=False,
            observed_regular_file=False,
            unresolved_gaps=["artifact_missing"],
        )
    if not artifact.is_file():
        return WorkspaceFileVerificationEvidence(
            status="failed",
            artifact_path_fingerprint=path_fingerprint,
            observed_exists=True,
            observed_regular_file=False,
            unresolved_gaps=["artifact_not_regular_file"],
        )

    try:
        size_bytes = artifact.stat().st_size
    except OSError:
        return WorkspaceFileVerificationEvidence(
            status="failed",
            artifact_path_fingerprint=path_fingerprint,
            observed_exists=True,
            observed_regular_file=True,
            unresolved_gaps=["artifact_stat_failed"],
        )
    gaps: list[str] = []
    if size_bytes < contract.min_bytes:
        gaps.append("artifact_smaller_than_minimum")
    if size_bytes > contract.max_bytes:
        gaps.append("artifact_larger_than_maximum")
    try:
        sha256 = _file_sha256(artifact) if size_bytes <= contract.max_bytes else None
    except OSError:
        gaps.append("artifact_hash_failed")
        sha256 = None
    if contract.expected_sha256 is not None and sha256 != contract.expected_sha256:
        gaps.append("artifact_sha256_mismatch")

    return WorkspaceFileVerificationEvidence(
        status="passed" if not gaps else "failed",
        artifact_path_fingerprint=path_fingerprint,
        observed_exists=True,
        observed_regular_file=True,
        observed_size_bytes=size_bytes,
        observed_sha256=sha256,
        unresolved_gaps=gaps,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
