from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RuntimeActionContract:
    run_id: str
    step_id: str
    kind: str
    title: str = ""


_ACTIONABLE_PLAN_STEP_KINDS = {"octo", "tool", "worker"}
_RESOLVED_PLAN_STEP_STATUSES = {
    "awaiting_worker",
    "awaiting_approval",
    "awaiting_user",
    "completed",
    "failed",
    "skipped",
    "cancelled",
    "blocked",
}


def _update_runtime_action_contracts(
    contracts: list[RuntimeActionContract],
    *,
    tool_name: str,
    tool_result: Any,
) -> list[RuntimeActionContract]:
    structured = _coerce_tool_result_dict(tool_result)
    if structured is None:
        return contracts

    remaining = list(contracts)
    if tool_name in {"start_worker", "start_child_worker"}:
        remaining = _resolve_contracts_from_worker_launch(remaining, structured)
    elif tool_name == "start_workers_parallel":
        remaining = _resolve_contracts_from_parallel_worker_launch(remaining, structured)
    elif tool_name == "plan_update_step":
        remaining = _resolve_contracts_from_plan_snapshot(remaining, structured)

    for contract in _contracts_created_by_tool_result(tool_name, structured):
        if contract not in remaining:
            remaining.append(contract)
    return remaining


def _coerce_tool_result_dict(tool_result: Any) -> dict[str, Any] | None:
    structured = tool_result
    if isinstance(tool_result, str):
        try:
            structured = json.loads(tool_result)
        except Exception:
            return None
    if isinstance(structured, dict):
        return structured
    return None


def _contracts_created_by_tool_result(
    tool_name: str,
    structured: dict[str, Any],
) -> list[RuntimeActionContract]:
    if tool_name not in {"plan_create", "plan_update_step"}:
        return []
    if str(structured.get("status") or "").lower() != "ok":
        return []
    snapshot = structured.get("snapshot")
    if not isinstance(snapshot, dict):
        return []
    next_step = snapshot.get("next_step")
    if not isinstance(next_step, dict):
        return []
    kind = str(next_step.get("kind") or "").strip().lower()
    status = str(next_step.get("status") or "").strip().lower()
    if kind not in _ACTIONABLE_PLAN_STEP_KINDS or status != "pending":
        return []
    run = snapshot.get("run") if isinstance(snapshot.get("run"), dict) else {}
    run_id = str(run.get("id") or next_step.get("run_id") or "").strip()
    step_id = str(next_step.get("step_id") or next_step.get("id") or "").strip()
    if not run_id or not step_id:
        return []
    return [
        RuntimeActionContract(
            run_id=run_id,
            step_id=step_id,
            kind=kind,
            title=str(next_step.get("title") or "").strip(),
        )
    ]


def _resolve_contracts_from_worker_launch(
    contracts: list[RuntimeActionContract],
    structured: dict[str, Any],
) -> list[RuntimeActionContract]:
    plan_binding = structured.get("plan_binding")
    if not isinstance(plan_binding, dict):
        return contracts
    if str(plan_binding.get("status") or "").lower() != "ok":
        return contracts
    run_id = str(plan_binding.get("run_id") or "").strip()
    step_id = str(plan_binding.get("step_id") or "").strip()
    if not run_id or not step_id:
        return contracts
    return [
        contract
        for contract in contracts
        if not (contract.run_id == run_id and contract.step_id == step_id)
    ]


def _resolve_contracts_from_parallel_worker_launch(
    contracts: list[RuntimeActionContract],
    structured: dict[str, Any],
) -> list[RuntimeActionContract]:
    launches = structured.get("launches")
    if not isinstance(launches, list):
        return contracts
    remaining = list(contracts)
    for launch in launches:
        if isinstance(launch, dict):
            remaining = _resolve_contracts_from_worker_launch(remaining, launch)
    return remaining


def _resolve_contracts_from_plan_snapshot(
    contracts: list[RuntimeActionContract],
    structured: dict[str, Any],
) -> list[RuntimeActionContract]:
    if str(structured.get("status") or "").lower() != "ok":
        return contracts
    snapshot = structured.get("snapshot")
    if not isinstance(snapshot, dict):
        return contracts
    run = snapshot.get("run") if isinstance(snapshot.get("run"), dict) else {}
    if str(run.get("status") or "").lower() in {"completed", "failed", "cancelled", "blocked"}:
        run_id = str(run.get("id") or "").strip()
        return [contract for contract in contracts if contract.run_id != run_id]

    step_status_by_key: dict[tuple[str, str], str] = {}
    for step in snapshot.get("steps") or []:
        if not isinstance(step, dict):
            continue
        run_id = str(step.get("run_id") or run.get("id") or "").strip()
        step_id = str(step.get("step_id") or step.get("id") or "").strip()
        if run_id and step_id:
            step_status_by_key[(run_id, step_id)] = str(step.get("status") or "").lower()

    return [
        contract
        for contract in contracts
        if step_status_by_key.get((contract.run_id, contract.step_id), "pending")
        not in _RESOLVED_PLAN_STEP_STATUSES
    ]


def _runtime_action_contract_retry_prompt(contracts: list[RuntimeActionContract]) -> str:
    pending = "\n".join(
        (
            f"- plan_run_id={contract.run_id}, plan_step_id={contract.step_id}, "
            f"kind={contract.kind}, title={contract.title or '(untitled)'}"
        )
        for contract in contracts
    )
    return (
        "Runtime state still contains an actionable plan step that has not been started, "
        "blocked, marked awaiting input/approval, or completed. Continue this same turn by "
        "creating concrete runtime evidence for the pending step.\n"
        f"{pending}\n"
        "Valid resolutions include:\n"
        "- for a worker step: call start_worker with the matching plan_run_id and plan_step_id;\n"
        "- for a tool or octo step: perform the tool/runtime work, then call plan_update_step;\n"
        "- if execution is impossible now: call plan_update_step with blocked, failed, awaiting_user, "
        "or awaiting_approval.\n"
        "Do not send a final user-visible status until the runtime state reflects one of those outcomes."
    )


def _runtime_action_contract_blocked_response(contracts: list[RuntimeActionContract]) -> str:
    pending = ", ".join(f"{contract.run_id}/{contract.step_id}" for contract in contracts)
    return (
        "I created runtime plan state, but no executor or terminal state was recorded for "
        f"the pending step(s): {pending}. I am stopping here instead of claiming that work "
        "has started without runtime evidence."
    )


def _tool_result_requests_followup(tool_name: str | None, tool_result: Any) -> bool:
    del tool_name
    structured = _coerce_tool_result_dict(tool_result)
    if structured is None:
        return False
    return bool(structured.get("followup_required"))
