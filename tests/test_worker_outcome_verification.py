from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from pydantic import ValidationError

from octopal.runtime.memory.episodes import build_worker_execution_episode
from octopal.runtime.workers.agent_worker import _build_outcome_verification_prompt
from octopal.runtime.workers.contracts import (
    WorkerResult,
    WorkerSpec,
    WorkspaceFileVerificationContract,
)
from octopal.runtime.workers.outcomes import verify_worker_outcome
from octopal.tools.filesystem.files import fs_write
from octopal.tools.workers.management import get_worker_tools


def _spec(
    contract: WorkspaceFileVerificationContract | None,
    *,
    allowed_paths: list[str] | None = None,
) -> WorkerSpec:
    return WorkerSpec(
        id="worker-1",
        task="Create a report",
        inputs={},
        system_prompt="Create the requested report.",
        available_tools=["fs_write"],
        granted_capabilities=[],
        timeout_seconds=10,
        max_thinking_steps=2,
        allowed_paths=allowed_paths,
        outcome_verification=contract,
    )


def test_workspace_file_verifier_records_independent_success_metadata(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    artifact = workspace / "reports" / "result.txt"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("verified report", encoding="utf-8")
    expected_digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    contract = WorkspaceFileVerificationContract(
        artifact_path="reports/result.txt",
        min_bytes=4,
        expected_sha256=expected_digest,
    )
    spec = _spec(contract, allowed_paths=["reports"])
    result = WorkerResult(summary="done", output={"report": "created"})

    evidence = verify_worker_outcome(
        spec=spec,
        result=result,
        worker_status="completed",
        workspace_dir=workspace,
    )
    assert evidence is not None
    assert evidence.status == "passed"
    assert evidence.observed_size_bytes == len("verified report")
    assert evidence.observed_sha256 == expected_digest

    episode = build_worker_execution_episode(
        spec=spec,
        result=result,
        stored_output=result.output,
        status="completed",
        launcher_kind="TestLauncher",
        outcome_evidence=evidence,
    )

    assert episode.verification["protocol_valid"] is True
    assert episode.verification["task_completed"] is True
    assert episode.verification["task_completed_basis"] == "worker_report_with_structured_output"
    assert episode.verification["verified"] is True
    assert episode.verification["accepted"] is False
    assert episode.verification["grader_results"][0]["verifier"] == "workspace_file"
    assert "reports/result.txt" not in episode.model_dump_json()


def test_workspace_file_contract_uses_shared_workspace_path(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    worker_dir = workspace / "workers" / "worker-1"
    worker_dir.mkdir(parents=True)
    contract = WorkspaceFileVerificationContract(artifact_path="reports/result")
    spec = _spec(contract)

    assert spec.allowed_paths == ["reports"]
    assert (
        fs_write(
            {"path": contract.artifact_path, "content": "verified report"},
            {
                "base_dir": worker_dir,
                "workspace_root": workspace,
                "worker": type("Worker", (), {"spec": spec})(),
            },
        )
        == "fs_write ok"
    )
    assert (workspace / contract.artifact_path).is_file()
    assert not (worker_dir / contract.artifact_path).exists()

    evidence = verify_worker_outcome(
        spec=spec,
        result=WorkerResult(summary="done", output={"report": "created"}),
        worker_status="completed",
        workspace_dir=workspace,
    )
    assert evidence is not None
    assert evidence.status == "passed"


def test_outcome_prompt_exposes_the_exact_artifact_contract() -> None:
    contract = WorkspaceFileVerificationContract(
        artifact_path="reports/result.txt",
        min_bytes=10,
        max_bytes=100,
        expected_sha256="a" * 64,
    )

    prompt = _build_outcome_verification_prompt(_spec(contract))

    assert "reports/result.txt" in prompt
    assert "10 to 100 bytes" in prompt
    assert "a" * 64 in prompt


def test_workspace_file_verifier_does_not_trust_worker_completion_claim(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    spec = _spec(WorkspaceFileVerificationContract(artifact_path="reports/missing.txt"))
    result = WorkerResult(
        summary="done",
        output={"report": "claimed", "verification": {"passed": True}},
    )

    evidence = verify_worker_outcome(
        spec=spec,
        result=result,
        worker_status="completed",
        workspace_dir=workspace,
    )
    assert evidence is not None
    assert evidence.status == "failed"
    assert evidence.unresolved_gaps == ["artifact_missing"]

    episode = build_worker_execution_episode(
        spec=spec,
        result=result,
        stored_output=result.output,
        status="completed",
        launcher_kind="TestLauncher",
        outcome_evidence=evidence,
    )
    assert episode.verification["task_completed"] is True
    assert episode.verification["verified"] is False


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/report.txt",
        "C:\\temp\\report.txt",
        "reports/../secret.txt",
        "report.txt",
        "reports/",
    ],
)
def test_workspace_file_contract_rejects_non_workspace_paths(path: str) -> None:
    with pytest.raises(
        ValidationError, match="workspace-relative|traverse|inside a workspace directory"
    ):
        WorkspaceFileVerificationContract(artifact_path=path)


def test_worker_start_tools_expose_host_outcome_verification_contract() -> None:
    tools = {tool.name: tool for tool in get_worker_tools()}
    for tool_name in ("start_worker", "start_child_worker"):
        properties = tools[tool_name].parameters["properties"]
        contract = properties["outcome_verification"]
        assert contract["required"] == ["kind", "artifact_path"]
        assert contract["properties"]["kind"]["const"] == "workspace_file"
