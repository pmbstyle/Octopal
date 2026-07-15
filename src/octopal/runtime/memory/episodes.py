from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from octopal import __version__
from octopal.infrastructure.store.models import ExecutionEpisodeRecord
from octopal.runtime.workers.contracts import WorkerResult, WorkerSpec
from octopal.utils import utc_now


def build_worker_execution_episode(
    *,
    spec: WorkerSpec,
    result: WorkerResult,
    stored_output: dict[str, Any] | None,
    status: Literal["completed", "failed", "stopped"],
    launcher_kind: str,
    evidence_storage: Literal["metadata_only", "aes256gcm"] = "metadata_only",
) -> ExecutionEpisodeRecord:
    """Build a content-addressed evidence index without copying task or output values."""

    terminal_status = status
    result_payload = result.model_dump(mode="json")
    result_payload["output"] = stored_output
    result_fingerprint = _fingerprint(result_payload)
    output_keys = _keys(stored_output)
    domain_output_keys = [key for key in output_keys if not key.startswith("_")]
    telemetry = stored_output.get("_telemetry") if isinstance(stored_output, dict) else None
    explicit_verification = (
        stored_output.get("verification") if isinstance(stored_output, dict) else None
    )
    verification_keys = _keys(explicit_verification)
    llm_config = spec.llm_config
    model = (llm_config.model if llm_config else None) or spec.model
    provider_id = llm_config.provider_id if llm_config else None
    mcp_tool_refs = [
        {
            "server_id": str(tool.get("server_id") or ""),
            "name": str(tool.get("name") or ""),
        }
        for tool in spec.mcp_tools
        if isinstance(tool, dict)
    ]

    record_payload = {
        "worker_run_id": spec.id,
        "result_fingerprint": result_fingerprint,
        "status": terminal_status,
    }
    episode_id = f"episode_{_fingerprint(record_payload)}"

    return ExecutionEpisodeRecord(
        id=episode_id,
        worker_run_id=spec.id,
        task_fingerprint=worker_task_fingerprint(spec.task, spec.inputs),
        environment_fingerprint=_fingerprint(
            {"launcher_kind": launcher_kind, "lifecycle": spec.lifecycle}
        ),
        capability_fingerprint=worker_capability_fingerprint(
            granted_capabilities=spec.granted_capabilities,
            effective_permissions=spec.effective_permissions,
            available_tools=spec.available_tools,
            mcp_tools=mcp_tool_refs,
        ),
        result_fingerprint=result_fingerprint,
        status=terminal_status,
        source_kind="worker",
        trust_state="observed",
        correlation_id=spec.correlation_id,
        template_id=spec.template_id or None,
        model=model,
        trajectory_refs={
            "worker_record_id": spec.id,
            "audit_correlation_id": spec.id,
            "plan_step_lookup": "worker_run_id",
        },
        result_metadata={
            "summary_chars": len(result.summary),
            "output_key_count": len(output_keys),
            "output_keys_fingerprint": _fingerprint(output_keys),
            "questions_count": len(result.questions),
            "knowledge_proposals_count": len(result.knowledge_proposals),
            "thinking_steps": result.thinking_steps,
            "tools_used_count": len(set(result.tools_used)),
            "tools_used_fingerprint": _fingerprint(sorted(set(result.tools_used))),
        },
        verification={
            "terminal_status": terminal_status,
            "result_contract_validated": True,
            "structured_output_present": bool(domain_output_keys),
            "domain_output_key_count": len(domain_output_keys),
            "telemetry_present": isinstance(telemetry, dict),
            "explicit_verification_present": isinstance(explicit_verification, dict),
            "explicit_verification_key_count": len(verification_keys),
            "explicit_verification_keys_fingerprint": _fingerprint(verification_keys),
            "grader_results": [],
        },
        provenance={
            "source_ref": spec.id,
            "provider_id": provider_id,
            "prompt_fingerprint": _fingerprint(spec.system_prompt),
            "tool_catalog_fingerprint": _fingerprint(
                {"available_tools": spec.available_tools, "mcp_tools": mcp_tool_refs}
            ),
            "code_version": __version__,
            "result_storage": "workers.output_json",
            "content_policy": (
                "metadata_with_encrypted_raw_v1"
                if evidence_storage == "aes256gcm"
                else "metadata_only_v1"
            ),
            "evidence_storage": evidence_storage,
            "procedural_recipe_ids": [recipe.id for recipe in spec.procedural_recipes],
        },
        created_at=utc_now(),
    )


def _fingerprint(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def worker_task_fingerprint(task: str, inputs: dict[str, Any]) -> str:
    return _fingerprint({"task": task, "inputs": inputs})


def worker_capability_fingerprint(
    *,
    granted_capabilities: list[dict[str, Any]],
    effective_permissions: list[str],
    available_tools: list[str],
    mcp_tools: list[dict[str, Any]],
) -> str:
    mcp_tool_refs = [
        {
            "server_id": str(tool.get("server_id") or ""),
            "name": str(tool.get("name") or ""),
        }
        for tool in mcp_tools
        if isinstance(tool, dict)
    ]
    return _fingerprint(
        {
            "granted_capabilities": granted_capabilities,
            "effective_permissions": effective_permissions,
            "available_tools": available_tools,
            "mcp_tools": mcp_tool_refs,
        }
    )


def _keys(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return []
    return sorted(str(key) for key in value)
